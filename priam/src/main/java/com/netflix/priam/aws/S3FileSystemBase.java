/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractFileSystem;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public abstract class S3FileSystemBase extends AbstractFileSystem {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystemBase.class);
    S3Client s3Client;
    final IConfiguration config;
    final ICompression compress;
    final BlockingSubmitThreadPoolExecutor executor;
    final RateLimiter rateLimiter;
    private final RateLimiter objectExistLimiter;

    S3FileSystemBase(
            Provider<AbstractBackupPath> pathProvider,
            ICompression compress,
            final IConfiguration config,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr) {
        super(config, backupMetrics, backupNotificationMgr, pathProvider);
        this.compress = compress;
        this.config = config;

        int threads = config.getBackupThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(threads);
        this.executor =
                new BlockingSubmitThreadPoolExecutor(threads, queue, config.getUploadTimeout());

        // a throttling mechanism, we can limit the amount of bytes uploaded to endpoint per second.
        this.rateLimiter = RateLimiter.create(1);
        // a throttling mechanism, we can limit the amount of S3 API calls endpoint per second.
        this.objectExistLimiter = RateLimiter.create(1);
        configChangeListener();
    }

    /*
       Call this method to change the configuration in runtime via callback.
    */
    public void configChangeListener() {
        int objectExistLimit = config.getRemoteFileSystemObjectExistsThrottle();
        objectExistLimiter.setRate(objectExistLimit < 1 ? Double.MAX_VALUE : objectExistLimit);

        double throttleLimit = config.getUploadThrottle();
        rateLimiter.setRate(throttleLimit < 1 ? Double.MAX_VALUE : throttleLimit);

        logger.info(
                "Updating rateLimiters: s3UploadThrottle: {}, objectExistLimiter: {}",
                rateLimiter.getRate(),
                objectExistLimiter.getRate());
    }

    private S3Client getS3Client() {
        return s3Client;
    }

    /*
     * A means to change the default handle to the S3 client.
     */
    public void setS3Client(S3Client client) {
        s3Client = client;
    }

    void checkSuccessfulUpload(
            CompleteMultipartUploadResponse resultS3MultiPartUploadComplete, Path localPath)
            throws BackupRestoreException {
        if (null != resultS3MultiPartUploadComplete
                && null != resultS3MultiPartUploadComplete.eTag()) {
            logger.info(
                    "Uploaded file: {}, object eTag: {}",
                    localPath,
                    resultS3MultiPartUploadComplete.eTag());
        } else {
            throw new BackupRestoreException(
                    "Error uploading file as ETag or CompleteMultipartUploadResponse is NULL -"
                            + localPath);
        }
    }

    @Override
    public long getFileSize(String remotePath) throws BackupRestoreException {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(getShard())
                .key(remotePath)
                .build();
        HeadObjectResponse response = s3Client.headObject(request);
        return response.contentLength();
    }

    @Override
    protected boolean doesRemoteFileExist(Path remotePath) {
        objectExistLimiter.acquire();
        boolean exists = false;
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(getShard())
                    .key(remotePath.toString())
                    .build();
            s3Client.headObject(request);
            exists = true;
        } catch (NoSuchKeyException ex) {
            exists = false;
        } catch (SdkException ex) {
            // No point throwing this exception up.
            logger.error(
                    "Exception while checking existence of object: {}. Error: {}",
                    remotePath,
                    ex.getMessage());
        }

        return exists;
    }

    @Override
    public void shutdown() {
        if (executor != null) executor.shutdown();
    }

    @Override
    public Iterator<String> listFileSystem(String prefix, String delimiter, String marker) {
        return new S3Iterator(s3Client, getShard(), prefix, delimiter, marker);
    }

    @Override
    public void deleteFiles(List<Path> remotePaths) throws BackupRestoreException {
        if (remotePaths.isEmpty()) return;

        try {
            List<ObjectIdentifier> keys =
                    remotePaths
                            .stream()
                            .map(remotePath ->
                                    ObjectIdentifier.builder()
                                            .key(remotePath.toString())
                                            .build())
                            .collect(Collectors.toList());

            Delete delete = Delete.builder()
                    .objects(keys)
                    .quiet(true)
                    .build();

            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(getShard())
                    .delete(delete)
                    .build();

            s3Client.deleteObjects(request);
            logger.info("Deleted {} objects from S3", remotePaths.size());
        } catch (Exception e) {
            logger.error(
                    "Error while trying to delete [{}]  the objects from S3: {}",
                    remotePaths.size(),
                    e.getMessage());
            throw new BackupRestoreException(e + " while trying to delete the objects");
        }
    }
}
