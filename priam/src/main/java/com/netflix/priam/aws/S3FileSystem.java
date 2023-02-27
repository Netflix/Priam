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
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.DynamicRateLimiter;
import com.netflix.priam.backup.RangeReadInputStream;
import com.netflix.priam.compress.ChunkedStream;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import com.netflix.priam.utils.SystemUtils;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of IBackupFileSystem for S3 */
@Singleton
public class S3FileSystem extends S3FileSystemBase {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
    private static final long MAX_BUFFER_SIZE = 5L * 1024L * 1024L;
    private final DynamicRateLimiter dynamicRateLimiter;

    @Inject
    public S3FileSystem(
            @Named("awss3roleassumption") IS3Credential cred,
            Provider<AbstractBackupPath> pathProvider,
            ICompression compress,
            final IConfiguration config,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            InstanceInfo instanceInfo,
            DynamicRateLimiter dynamicRateLimiter) {
        super(pathProvider, compress, config, backupMetrics, backupNotificationMgr);
        s3Client =
                AmazonS3Client.builder()
                        .withCredentials(cred.getAwsCredentialProvider())
                        .withRegion(instanceInfo.getRegion())
                        .build();
        this.dynamicRateLimiter = dynamicRateLimiter;
    }

    @Override
    protected void downloadFileImpl(AbstractBackupPath path, String suffix)
            throws BackupRestoreException {
        String remotePath = path.getRemotePath();
        File localFile = new File(path.newRestoreFile().getAbsolutePath() + suffix);
        long size = super.getFileSize(remotePath);
        final int bufferSize = Math.toIntExact(Math.min(MAX_BUFFER_SIZE, size));
        try (BufferedInputStream is =
                        new BufferedInputStream(
                                new RangeReadInputStream(s3Client, getShard(), size, remotePath),
                                bufferSize);
                BufferedOutputStream os =
                        new BufferedOutputStream(new FileOutputStream(localFile))) {
            if (path.getCompression() == CompressionType.NONE) {
                IOUtils.copyLarge(is, os);
            } else {
                compress.decompressAndClose(is, os);
            }
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

    private long uploadMultipart(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        Path localPath = Paths.get(path.getBackupFile().getAbsolutePath());
        String remotePath = path.getRemotePath();
        long chunkSize = getChunkSize(localPath);
        String prefix = config.getBackupPrefix();
        if (logger.isDebugEnabled())
            logger.debug("Uploading to {}/{} with chunk size {}", prefix, remotePath, chunkSize);
        File localFile = localPath.toFile();
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(prefix, remotePath)
                        .withObjectMetadata(getObjectMetadata(localFile));
        String uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
        DataPart part = new DataPart(prefix, remotePath, uploadId);
        List<PartETag> partETags = Collections.synchronizedList(new ArrayList<>());

        try (InputStream in = new FileInputStream(localFile)) {
            Iterator<byte[]> chunks = new ChunkedStream(in, chunkSize, path.getCompression());
            int partNum = 0;
            AtomicInteger partsPut = new AtomicInteger(0);
            long compressedFileSize = 0;

            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                dynamicRateLimiter.acquire(path, target, chunk.length);
                DataPart dp = new DataPart(++partNum, chunk, prefix, remotePath, uploadId);
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags, partsPut);
                compressedFileSize += chunk.length;
                // TODO: output Future<Etag> instead, collect them here, wait for all below
                executor.submit(partUploader);
            }

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

    protected long uploadFileImpl(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
        if (localFile.length() >= config.getBackupChunkSize()) return uploadMultipart(path, target);
        byte[] chunk = getFileContents(path);
        rateLimiter.acquire(chunk.length);
        dynamicRateLimiter.acquire(path, target, chunk.length);
        try {
            new BoundedExponentialRetryCallable<PutObjectResult>(1000, 10000, 5) {
                @Override
                public PutObjectResult retriableCall() {
                    return s3Client.putObject(generatePut(path, chunk));
                }
            }.call();
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading file: " + localFile.getName(), e);
        }
        return chunk.length;
    }

    private PutObjectRequest generatePut(AbstractBackupPath path, byte[] chunk) {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
        ObjectMetadata metadata = getObjectMetadata(localFile);
        metadata.setContentLength(chunk.length);
        PutObjectRequest put =
                new PutObjectRequest(
                        config.getBackupPrefix(),
                        path.getRemotePath(),
                        new ByteArrayInputStream(chunk),
                        metadata);
        if (config.addMD5ToBackupUploads()) {
            put.getMetadata().setContentMD5(SystemUtils.toBase64(SystemUtils.md5(chunk)));
        }
        return put;
    }

    private byte[] getFileContents(AbstractBackupPath path) throws BackupRestoreException {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                InputStream in = new BufferedInputStream(new FileInputStream(localFile))) {
            Iterator<byte[]> chunks =
                    new ChunkedStream(in, config.getBackupChunkSize(), path.getCompression());
            while (chunks.hasNext()) {
                byteArrayOutputStream.write(chunks.next());
            }
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new BackupRestoreException("Error reading file: " + localFile.getName(), e);
        }
    }
}
