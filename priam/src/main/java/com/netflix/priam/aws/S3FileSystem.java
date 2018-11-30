/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.aws;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of IBackupFileSystem for S3 */
@Singleton
public class S3FileSystem extends S3FileSystemBase {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);

    @Inject
    public S3FileSystem(
            @Named("awss3roleassumption") IS3Credential cred,
            Provider<AbstractBackupPath> pathProvider,
            ICompression compress,
            final IConfiguration config,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            InstanceInfo instanceInfo) {
        super(pathProvider, compress, config, backupMetrics, backupNotificationMgr);
        s3Client =
                AmazonS3Client.builder()
                        .withCredentials(cred.getAwsCredentialProvider())
                        .withRegion(instanceInfo.getRegion())
                        .build();
    }

    @Override
    protected void downloadFileImpl(Path remotePath, Path localPath) throws BackupRestoreException {
        try {
            long remoteFileSize = super.getFileSize(remotePath);
            RangeReadInputStream rris =
                    new RangeReadInputStream(
                            s3Client, getBucket(), remoteFileSize, remotePath.toString());
            final long bufSize =
                    MAX_BUFFERED_IN_STREAM_SIZE > remoteFileSize
                            ? remoteFileSize
                            : MAX_BUFFERED_IN_STREAM_SIZE;
            compress.decompressAndClose(
                    new BufferedInputStream(rris, (int) bufSize),
                    new BufferedOutputStream(new FileOutputStream(localPath.toFile())));
        } catch (Exception e) {
            throw new BackupRestoreException(
                    "Exception encountered downloading "
                            + remotePath
                            + " from S3 bucket "
                            + getBucket()
                            + ", Msg: "
                            + e.getMessage(),
                    e);
        }
    }

    private ObjectMetadata getObjectMetadata(Path path) {
        ObjectMetadata ret = new ObjectMetadata();
        long lastModified = path.toFile().lastModified();

        if (lastModified != 0) {
            ret.addUserMetadata("local-modification-time", Long.toString(lastModified));
        }

        long fileSize = path.toFile().length();
        if (fileSize != 0) {
            ret.addUserMetadata("local-size", Long.toString(fileSize));
        }
        return ret;
    }

    private long uploadMultipart(Path localPath, Path remotePath) throws BackupRestoreException {
        long chunkSize = getChunkSize(localPath);
        if (logger.isDebugEnabled())
            logger.debug(
                    "Uploading to {}/{} with chunk size {}",
                    config.getBackupPrefix(),
                    remotePath,
                    chunkSize);
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(config.getBackupPrefix(), remotePath.toString());
        initRequest.withObjectMetadata(getObjectMetadata(localPath));
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart part =
                new DataPart(
                        config.getBackupPrefix(),
                        remotePath.toString(),
                        initResponse.getUploadId());
        List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>());

        try (InputStream in = new FileInputStream(localPath.toFile())) {
            Iterator<byte[]> chunks = compress.compress(in, chunkSize);
            // Upload parts.
            int partNum = 0;
            AtomicInteger partsUploaded = new AtomicInteger(0);
            long compressedFileSize = 0;

            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                DataPart dp =
                        new DataPart(
                                ++partNum,
                                chunk,
                                config.getBackupPrefix(),
                                remotePath.toString(),
                                initResponse.getUploadId());
                S3PartUploader partUploader =
                        new S3PartUploader(s3Client, dp, partETags, partsUploaded);
                compressedFileSize += chunk.length;
                // TODO: Get the future over here and create a new arraylist.
                Future<Void> future = executor.submit(partUploader);
            }

            // TODO: Instead of waiting for executor thread to be empty we should wait for all the
            // futures to finish.
            executor.sleepTillEmpty();
            logger.info(
                    "All chunks uploaded for file {}, num of expected parts:{}, num of actual uploaded parts: {}",
                    localPath.toFile().getName(),
                    partNum,
                    partsUploaded.get());

            if (partNum != partETags.size())
                throw new BackupRestoreException(
                        "Number of parts("
                                + partNum
                                + ")  does not match the uploaded parts("
                                + partETags.size()
                                + ")");

            CompleteMultipartUploadResult resultS3MultiPartUploadComplete =
                    new S3PartUploader(s3Client, part, partETags).completeUpload();
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, localPath);

            if (logger.isDebugEnabled()) {
                final S3ResponseMetadata responseMetadata =
                        s3Client.getCachedResponseMetadata(initRequest);
                final String requestId =
                        responseMetadata.getRequestId(); // "x-amz-request-id" header
                final String hostId = responseMetadata.getHostId(); // "x-amz-id-2" header
                logger.debug(
                        "S3 AWS x-amz-request-id["
                                + requestId
                                + "], and x-amz-id-2["
                                + hostId
                                + "]");
            }

            return compressedFileSize;
        } catch (Exception e) {
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file: " + localPath.toString(), e);
        }
    }

    protected long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException {
        long chunkSize = config.getBackupChunkSize();
        long fileSize = localPath.toFile().length();

        if (fileSize < chunkSize) {
            // Upload file without using multipart upload as it will be more efficient.
            if (logger.isDebugEnabled())
                logger.debug(
                        "Uploading to {}/{} using PUT operation",
                        config.getBackupPrefix(),
                        remotePath);

            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    InputStream in =
                            new BufferedInputStream(new FileInputStream(localPath.toFile()))) {
                Iterator<byte[]> chunkedStream = compress.compress(in, chunkSize);
                while (chunkedStream.hasNext()) {
                    byteArrayOutputStream.write(chunkedStream.next());
                }
                byte[] chunk = byteArrayOutputStream.toByteArray();
                long compressedFileSize = chunk.length;
                rateLimiter.acquire(chunk.length);
                ObjectMetadata objectMetadata = getObjectMetadata(localPath);
                objectMetadata.setContentLength(chunk.length);
                PutObjectRequest putObjectRequest =
                        new PutObjectRequest(
                                config.getBackupPrefix(),
                                remotePath.toString(),
                                new ByteArrayInputStream(chunk),
                                objectMetadata);
                // Retry if failed.
                PutObjectResult upload =
                        new BoundedExponentialRetryCallable<PutObjectResult>(1000, 10000, 5) {
                            @Override
                            public PutObjectResult retriableCall() throws Exception {
                                return s3Client.putObject(putObjectRequest);
                            }
                        }.call();

                if (logger.isDebugEnabled())
                    logger.debug(
                            "Successfully uploaded file with putObject: {} and etag: {}",
                            remotePath,
                            upload.getETag());

                return compressedFileSize;
            } catch (Exception e) {
                throw new BackupRestoreException(
                        "Error uploading file: " + localPath.toFile().getName(), e);
            }
        } else return uploadMultipart(localPath, remotePath);
    }
}
