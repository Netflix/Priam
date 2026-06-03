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

import com.google.common.annotations.VisibleForTesting;
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
import com.netflix.priam.utils.ByteBufferInputStream;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Implementation of IBackupFileSystem for S3 */
@Singleton
public class S3FileSystem extends S3FileSystemBase {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
    private static final int MAX_CHUNKS = 9995; // 10K is AWS limit, minus a small buffer
    private static final long MAX_BUFFER_SIZE = 5L * 1024L * 1024L;

    private final DynamicRateLimiter dynamicRateLimiter;
    private final ThreadLocal<ByteBuffer> inputBufferThreadLocal = new ThreadLocal<>();
    private final ThreadLocal<ByteBuffer> compressBufferThreadLocal = new ThreadLocal<>();

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

    static long getChunkSize(Path path, long minimumChunkSize) {
        return Math.max(path.toFile().length() / MAX_CHUNKS, minimumChunkSize);
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
        if (config.useReusableBufferForMultipartUploads() && path.getCompression() == CompressionType.NONE) {
            return uploadMultipartWithBuffers(path, target);
        } else {
            return uploadMultipartLegacy(path, target);
        }
    }

    private long uploadMultipartWithBuffers(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        Path localPath = Paths.get(path.getBackupFile().getAbsolutePath());
        String remotePath = path.getRemotePath();
        long chunkSize = getChunkSize(localPath, config.getBackupChunkSize());
        String prefix = config.getBackupPrefix();
        if (logger.isDebugEnabled())
            logger.debug("Uploading to {}/{} with chunk size {} (using ByteBuffer implementation)", prefix, remotePath, chunkSize);
        File localFile = localPath.toFile();

        CreateMultipartUploadRequest.Builder initRequestBuilder = CreateMultipartUploadRequest.builder()
                .bucket(prefix)
                .key(remotePath);

        Map<String, String> metadata = getFileMetadata(localFile);
        if (!metadata.isEmpty()) {
            initRequestBuilder.metadata(metadata);
        }

        CreateMultipartUploadRequest initRequest = initRequestBuilder.build();
        String uploadId = s3Client.createMultipartUpload(initRequest).uploadId();
        List<CompletedPart> completedParts = new ArrayList<>();

        try (BufferIterator bufferIterator = new BufferIterator(inputBufferThreadLocal, compressBufferThreadLocal, path, config.getBackupChunkSize())) {
            int partNum = 0;
            long compressedFileSize = 0;
            while (bufferIterator.hasNext()) {
                ByteBuffer uploadBuffer = bufferIterator.next();
                int uploadSize = uploadBuffer.limit();
                rateLimiter.acquire(uploadSize);
                dynamicRateLimiter.acquire(path, target, uploadSize);
                partNum++;
                UploadPartRequest.Builder req = UploadPartRequest.builder()
                        .bucket(prefix)
                        .key(remotePath)
                        .uploadId(uploadId)
                        .partNumber(partNum)
                        .contentLength((long) uploadSize);
                byte[] md5 = SystemUtils.md5(uploadBuffer);
                if (config.addMD5ToBackupUploads()) {
                    req.contentMD5(SystemUtils.toBase64(md5));
                }
                UploadPartResponse uploadResult = new BoundedExponentialRetryCallable<UploadPartResponse>(200, 10000, 5) {
                    @Override
                    public UploadPartResponse retriableCall() {
                        try (ByteBufferInputStream inputStream = new ByteBufferInputStream(uploadBuffer)) {
                            return s3Client.uploadPart(
                                    req.build(),
                                    RequestBody.fromInputStream(inputStream, uploadSize));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }.call();

                String eTag = uploadResult.eTag();
                validateUpload(eTag, md5, partNum);
                completedParts.add(CompletedPart.builder().partNumber(partNum).eTag(eTag).build());
                compressedFileSize += uploadSize;

                if (logger.isDebugEnabled()) {
                    logger.debug("Uploaded part {} of size {}", partNum, uploadSize);
                }
            }
            logger.info("{} done. part count: {}", localFile, partNum);
            CompleteMultipartUploadResponse multipartUploadResponse =
                    s3Client.completeMultipartUpload(
                            CompleteMultipartUploadRequest.builder()
                                    .bucket(prefix)
                                    .key(remotePath)
                                    .uploadId(uploadId)
                                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                                    .build());
            checkSuccessfulUpload(multipartUploadResponse, localPath);

            return compressedFileSize;
        } catch (Exception e) {
            s3Client.abortMultipartUpload(
                    AbortMultipartUploadRequest.builder().bucket(prefix).key(remotePath).uploadId(uploadId).build());
            throw new BackupRestoreException("Error uploading file: " + localPath.toString(), e);
        }
    }

    private void validateUpload(String eTag, byte[] md5, int partNumber) throws SdkException, BackupRestoreException {
        // AWS SDK v2 returns ETags without quotes, but we need to compare the MD5 hex
        String expectedMd5 = SystemUtils.toHex(md5);
        String actualETag = eTag;

        if (actualETag != null && actualETag.startsWith("\"") && actualETag.endsWith("\"")) {
            actualETag = actualETag.substring(1, actualETag.length() - 1);
        }

        if (actualETag != null && actualETag.contains("-")) {
            actualETag = actualETag.substring(0, actualETag.indexOf("-"));
        }

        if (!actualETag.equals(expectedMd5)) {
            logger.error("MD5 mismatch for part {}: expected={}, actual={}",
                    partNumber, expectedMd5, actualETag);
            throw new BackupRestoreException(
                    "Unable to match MD5 for part " + partNumber);
        }
    }

    private long uploadMultipartLegacy(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        Path localPath = Paths.get(path.getBackupFile().getAbsolutePath());
        String remotePath = path.getRemotePath();
        long chunkSize = getChunkSize(localPath, config.getBackupChunkSize());
        String prefix = config.getBackupPrefix();
        if (logger.isDebugEnabled())
            logger.debug("Uploading to {}/{} with chunk size {} (using legacy implementation)", prefix, remotePath, chunkSize);
        File localFile = localPath.toFile();

        CreateMultipartUploadRequest.Builder initRequestBuilder = CreateMultipartUploadRequest.builder()
                .bucket(prefix)
                .key(remotePath);

        Map<String, String> metadata = getFileMetadata(localFile);
        if (!metadata.isEmpty()) {
            initRequestBuilder.metadata(metadata);
        }

        CreateMultipartUploadRequest initRequest = initRequestBuilder.build();
        String uploadId = s3Client.createMultipartUpload(initRequest).uploadId();
        DataPart part = new DataPart(prefix, remotePath, uploadId);
        List<CompletedPart> partETags = Collections.synchronizedList(new ArrayList<>());

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
                executor.submit(partUploader);
            }

            executor.sleepTillEmpty();
            logger.info("{} done. part count: {} expected: {}", localFile, partsPut.get(), partNum);
            Preconditions.checkState(partNum == partETags.size(), "part count mismatch");
            CompleteMultipartUploadResponse resultS3MultiPartUploadComplete =
                    new S3PartUploader(s3Client, part, partETags).completeUpload();
            checkSuccessfulUpload(resultS3MultiPartUploadComplete, localPath);

            return compressedFileSize;
        } catch (Exception e) {
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file: " + localPath.toString(), e);
        }
    }

    protected long uploadFileImpl(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();

        if (localFile.length() >= config.getBackupChunkSize())
            return uploadMultipart(path, target);

        if (config.useReusableBufferForMultipartUploads() && path.getCompression() == CompressionType.NONE) {
            return uploadFileWithByteBuffer(path, target);
        } else {
            return uploadFileWithByteArray(path, target);
        }
    }

    private long uploadFileWithByteBuffer(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
        ByteBuffer buffer = getFileByteBuffer(path);
        // C* snapshots may have empty files. That is probably unintentional.
        if (buffer.remaining() > 0) {
            rateLimiter.acquire(buffer.remaining());
            dynamicRateLimiter.acquire(path, target, buffer.remaining());
        }
        try {
            new BoundedExponentialRetryCallable<PutObjectResponse>(1000, 10000, 5) {
                @Override
                public PutObjectResponse retriableCall() {
                    try (ByteBufferInputStream inputStream = new ByteBufferInputStream(buffer)) {
                        return s3Client.putObject(
                                generatePutFromBuffer(path, buffer),
                                RequestBody.fromInputStream(inputStream, buffer.remaining()));
                    } catch (IOException e) {
                       throw new RuntimeException(e) ;
                    }
                }
            }.call();
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading file: " + localFile.getName(), e);
        }
        return buffer.remaining();
    }

    private long uploadFileWithByteArray(AbstractBackupPath path, Instant target)
            throws BackupRestoreException {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
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
                    return s3Client.putObject(generatePutFromByteArray(path, chunk), RequestBody.fromBytes(chunk));
                }
            }.call();
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading file: " + localFile.getName(), e);
        }
        return chunk.length;
    }

    private PutObjectRequest generatePutFromBuffer(AbstractBackupPath path, ByteBuffer buffer) {
        File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
        PutObjectRequest.Builder put =
                PutObjectRequest.builder()
                        .bucket(config.getBackupPrefix())
                        .key(path.getRemotePath())
                        .contentLength((long) buffer.remaining());
        Map<String, String> metadata = getFileMetadata(localFile);
        if (!metadata.isEmpty()) {
            put.metadata(metadata);
        }
        if (config.addMD5ToBackupUploads()) {
            put.contentMD5(SystemUtils.toBase64(SystemUtils.md5(buffer)));
        }
        return put.build();
    }

    private PutObjectRequest generatePutFromByteArray(AbstractBackupPath path, byte[] chunk) {
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

    @VisibleForTesting
    public ByteBuffer getFileByteBuffer(AbstractBackupPath path) throws BackupRestoreException {
        try (BufferIterator bufferIterator = new BufferIterator(
                inputBufferThreadLocal,
                compressBufferThreadLocal,
                path,
                config.getBackupChunkSize())) {
            return bufferIterator.next();
        } catch (Exception e) {
            File localFile = Paths.get(path.getBackupFile().getAbsolutePath()).toFile();
            throw new BackupRestoreException("Error reading file: " + localFile.getName(), e);
        }
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
