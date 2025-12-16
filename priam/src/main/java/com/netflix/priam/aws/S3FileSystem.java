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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
                S3Client.builder()
                        .credentialsProvider(cred.getAwsCredentialProvider())
                        .region(Region.of(instanceInfo.getRegion()))
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

    @Override
    public void putObject(String bucket, String key, String value) {
        String md5 = SystemUtils.toBase64(SystemUtils.md5(value.getBytes()));
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).contentMD5(md5).build(),
                RequestBody.fromBytes(value.getBytes()));
    }

    private Map<String, String> getFileMetadata(File file) {
            Map<String, String> metadata = new HashMap<>();
        long lastModified = file.lastModified();
        long fileSize = file.length();

        if (lastModified != 0) {
            metadata.put("local-modification-time", Long.toString(lastModified));
        }
        if (fileSize != 0) {
            metadata.put("local-size", Long.toString(fileSize));
        }
        return metadata;
    }

    private long uploadMultipart(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        Path localPath = Paths.get(path.getBackupFile().getAbsolutePath());
        String remotePath = path.getRemotePath();
        long chunkSize = getChunkSize(localPath);
        String prefix = config.getBackupPrefix();
        System.out.println(String.format("@@@ Uploading to {}/{} with chunk size {}", prefix, remotePath, chunkSize));
        if (logger.isDebugEnabled())
            logger.debug("Uploading to {}/{} with chunk size {}", prefix, remotePath, chunkSize);
        File localFile = localPath.toFile();

        System.out.println("@@@ creating multipart upload request");
        CreateMultipartUploadRequest.Builder initRequestBuilder = CreateMultipartUploadRequest.builder()
                .bucket(prefix)
                .key(remotePath);

        System.out.println("@@@ adding file metadata");
        Map<String, String> metadata = getFileMetadata(localFile);
        if (!metadata.isEmpty()) {
            initRequestBuilder.metadata(metadata);
        }

        System.out.println("@@@ creating upload and getting id");
        CreateMultipartUploadRequest initRequest = initRequestBuilder.build();
        String uploadId = s3Client.createMultipartUpload(initRequest).uploadId();
        System.out.println("@@@ wrapping in DataPart");
        DataPart part = new DataPart(prefix, remotePath, uploadId);
        List<CompletedPart> partETags = Collections.synchronizedList(new ArrayList<>());

        try (InputStream in = new FileInputStream(localFile)) {
            System.out.println("@@@ creating chunked stream");
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

            System.out.println("@@@ waiting until complete");
            executor.sleepTillEmpty();
            System.out.println("@@@ " + localFile + " done. part count: " + partsPut.get() + " expected: " + partNum);
            logger.info("{} done. part count: {} expected: {}", localFile, partsPut.get(), partNum);
            Preconditions.checkState(partNum == partETags.size(), "part count mismatch");
            System.out.println(String.format("@@@ complete multipart upload"));
            CompleteMultipartUploadResponse resultS3MultiPartUploadComplete =
                    new S3PartUploader(s3Client, part, partETags).completeUpload();
            System.out.println(String.format("@@@ check successful"));
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, localPath);

            System.out.println(String.format("@@@ return file size {}", compressedFileSize));
            return compressedFileSize;
        } catch (Exception e) {
            System.out.println("@@@ error uploading multipart file " + localPath.toFile() + ": " + e.getMessage());
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file: " + localPath.toString(), e);
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
            new BoundedExponentialRetryCallable<PutObjectResponse>(1000, 10000, 5) {
                @Override
                public PutObjectResponse retriableCall() {
                    return s3Client.putObject(generatePut(path, chunk), RequestBody.fromBytes(chunk));
                }
            }.call();
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading file: " + localFile.getName(), e);
        }
        return chunk.length;
    }

    private PutObjectRequest generatePut(AbstractBackupPath path, byte[] chunk) {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();

        PutObjectRequest.Builder builder = PutObjectRequest.builder()
                .bucket(config.getBackupPrefix())
                .key(path.getRemotePath())
                .contentLength((long) chunk.length);

        Map<String, String> metadata = getFileMetadata(localFile);
        if (!metadata.isEmpty()) {
            builder.metadata(metadata);
        }

        if (config.addMD5ToBackupUploads()) {
            builder.contentMD5(SystemUtils.toBase64(SystemUtils.md5(chunk)));
        }
        return builder.build();
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
