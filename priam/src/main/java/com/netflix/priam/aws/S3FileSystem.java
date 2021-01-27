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
import com.google.common.base.Preconditions;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of IBackupFileSystem for S3 */
@Singleton
public class S3FileSystem extends S3FileSystemBase {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
    private static final long MAX_BUFFER_SIZE = 5 * 1024 * 1024;

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
        String remotePathName = remotePath.toString();
        try {
            long size = super.getFileSize(remotePath);
            RangeReadInputStream rris =
                    new RangeReadInputStream(s3Client, getShard(), size, remotePathName);
            final long bufferSize = Math.min(MAX_BUFFER_SIZE, size);
            compress.decompressAndClose(
                    new BufferedInputStream(rris, (int) bufferSize),
                    new BufferedOutputStream(new FileOutputStream(localPath.toFile())));
        } catch (Exception e) {
            String err =
                    String.format(
                            "Failed to GET %s Bucket: %s Msg: %s",
                            remotePath, getShard(), e.getMessage());
            throw new BackupRestoreException(err);
        }
    }

    private ObjectMetadata getObjectMetadata(File file) {
        ObjectMetadata ret = new ObjectMetadata();
        long lastModified = file.lastModified();

        if (lastModified != 0) {
            ret.addUserMetadata("local-modification-time", Long.toString(lastModified));
        }

        long fileSize = file.length();
        if (fileSize != 0) {
            ret.addUserMetadata("local-size", Long.toString(fileSize));
        }
        return ret;
    }

    private long uploadMultipart(Path localPath, Path remotePath) throws BackupRestoreException {
        long chunkSize = getChunkSize(localPath);
        String prefix = config.getBackupPrefix();
        String destination = remotePath.toString();
        if (logger.isDebugEnabled())
            logger.debug("Uploading to {}/{} with chunk size {}", prefix, destination, chunkSize);
        File localFile = localPath.toFile();
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(prefix, remotePath.toString())
                        .withObjectMetadata(getObjectMetadata(localFile));
        String uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
        DataPart part = new DataPart(prefix, destination, uploadId);
        List<PartETag> partETags = Collections.synchronizedList(new ArrayList<>());

        try (InputStream in = new FileInputStream(localFile)) {
            Iterator<byte[]> chunks = compress.compress(in, chunkSize);
            // Upload parts.
            int partNum = 0;
            AtomicInteger partsPut = new AtomicInteger(0);
            long compressedFileSize = 0;

            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                DataPart dp = new DataPart(++partNum, chunk, prefix, destination, uploadId);
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags, partsPut);
                compressedFileSize += chunk.length;
                // TODO: Get the future over here and create a new arraylist.
                executor.submit(partUploader);
            }

            // TODO: Instead of waiting for executor thread to be empty we should wait for all the
            // futures to finish.
            executor.sleepTillEmpty();
            logger.info("{} done. part count: {} expected: {}", localFile, partsPut.get(), partNum);
            Preconditions.checkState(partNum == partETags.size(), "part count mismatch");
            CompleteMultipartUploadResult resultS3MultiPartUploadComplete =
                    new S3PartUploader(s3Client, part, partETags).completeUpload();
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, localPath);

            if (logger.isDebugEnabled()) {
                final S3ResponseMetadata info = s3Client.getCachedResponseMetadata(initRequest);
                logger.debug("Request Id: {}, Host Id: {}", info.getRequestId(), info.getHostId());
            }

            return compressedFileSize;
        } catch (Exception e) {
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file: " + localPath.toString(), e);
        }
    }

    protected long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException {
        long chunkSize = config.getBackupChunkSize();
        File localFile = localPath.toFile();
        if (localFile.length() >= chunkSize) return uploadMultipart(localPath, remotePath);

        String prefix = config.getBackupPrefix();
        String destination = remotePath.toString();
        // Upload file without using multipart upload as it will be more efficient.
        if (logger.isDebugEnabled()) logger.debug("PUTing {}/{}", prefix, destination);

        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                InputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            Iterator<byte[]> chunkedStream = compress.compress(in, chunkSize);
            while (chunkedStream.hasNext()) {
                byteArrayOutputStream.write(chunkedStream.next());
            }
            byte[] chunk = byteArrayOutputStream.toByteArray();
            long compressedFileSize = chunk.length;
            /**
             * Weird, right that we are checking for length which is positive. You can thanks this
             * to sometimes C* creating files which are zero bytes, and giving that in snapshot for
             * some unknown reason.
             */
            if (chunk.length > 0) rateLimiter.acquire(chunk.length);
            ObjectMetadata objectMetadata = getObjectMetadata(localFile);
            objectMetadata.setContentLength(chunk.length);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(chunk);
            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(prefix, destination, inputStream, objectMetadata);
            // Retry if failed.
            PutObjectResult upload =
                    new BoundedExponentialRetryCallable<PutObjectResult>(1000, 10000, 5) {
                        @Override
                        public PutObjectResult retriableCall() {
                            return s3Client.putObject(putObjectRequest);
                        }
                    }.call();
            if (logger.isDebugEnabled())
                logger.debug("Put: {} with etag: {}", remotePath, upload.getETag());
            return compressedFileSize;
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading file: " + localFile.getName(), e);
        }
    }
}
