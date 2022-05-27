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
import com.amazonaws.services.s3.model.*;
import com.google.api.client.util.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;

/** Implementation of IBackupFileSystem for S3 */
@Singleton
public class S3FileSystem extends S3FileSystemBase {
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
        S3MultipartManager manager =
                S3MultipartManager.create(
                        s3Client,
                        config.getBackupPrefix(),
                        path.getRemotePath(),
                        getObjectMetadata(localPath.toFile()));

        try (InputStream in = new FileInputStream(localPath.toFile())) {
            Iterator<byte[]> chunks =
                    new ChunkedStream(in, getChunkSize(localPath), path.getCompression());
            int partNum = 0;
            long compressedFileSize = 0;
            List<Future<PartETag>> eTagFutures = Lists.newArrayList();
            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                dynamicRateLimiter.acquire(path, target, chunk.length);
                compressedFileSize += chunk.length;
                eTagFutures.add(executor.submit(manager.getUploadTask(++partNum, chunk)));
            }
            List<PartETag> partETags = Lists.newArrayList();
            for (Future<PartETag> future : eTagFutures) {
                partETags.add(future.get());
            }
            checkSuccessfulUpload(manager.completeUpload(partETags), localPath);
            return compressedFileSize;
        } catch (Exception e) {
            manager.abortUpload();
            throw new BackupRestoreException("Error uploading file: " + localPath, e);
        }
    }

    protected long uploadFileImpl(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
        if (localFile.length() >= config.getBackupChunkSize()) return uploadMultipart(path, target);
        byte[] chunk = getFileContents(path);
        // C* snapshots may have empty files. That is probably unintentional.
        if (chunk.length > 0) {
            rateLimiter.acquire(chunk.length);
            dynamicRateLimiter.acquire(path, target, chunk.length);
        }
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
