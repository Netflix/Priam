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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Provider;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class S3FileSystemBase extends AbstractFileSystem {
    private static final int MAX_CHUNKS = 10000;
    static final long MAX_BUFFERED_IN_STREAM_SIZE = 5 * 1024 * 1024;
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystemBase.class);
    AmazonS3 s3Client;
    final IConfiguration config;
    final ICompression compress;
    final BlockingSubmitThreadPoolExecutor executor;
    // a throttling mechanism, we can limit the amount of bytes uploaded to endpoint per second.
    final RateLimiter rateLimiter;

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

        double throttleLimit = config.getUploadThrottle();
        this.rateLimiter = RateLimiter.create(throttleLimit < 1 ? Double.MAX_VALUE : throttleLimit);
    }

    private AmazonS3 getS3Client() {
        return s3Client;
    }

    /*
     * A means to change the default handle to the S3 client.
     */
    public void setS3Client(AmazonS3 client) {
        s3Client = client;
    }

    @Override
    public void cleanup() {

        AmazonS3 s3Client = getS3Client();
        String clusterPath = pathProvider.get().clusterPrefix("");
        logger.debug("Bucket: {}", config.getBackupPrefix());
        BucketLifecycleConfiguration lifeConfig =
                s3Client.getBucketLifecycleConfiguration(config.getBackupPrefix());
        logger.debug("Got bucket:{} lifecycle.{}", config.getBackupPrefix(), lifeConfig);
        if (lifeConfig == null) {
            lifeConfig = new BucketLifecycleConfiguration();
            List<Rule> rules = Lists.newArrayList();
            lifeConfig.setRules(rules);
        }
        List<Rule> rules = lifeConfig.getRules();
        if (updateLifecycleRule(config, rules, clusterPath)) {
            if (rules.size() > 0) {
                lifeConfig.setRules(rules);
                s3Client.setBucketLifecycleConfiguration(config.getBackupPrefix(), lifeConfig);
            } else s3Client.deleteBucketLifecycleConfiguration(config.getBackupPrefix());
        }
    }

    private boolean updateLifecycleRule(IConfiguration config, List<Rule> rules, String prefix) {
        Rule rule = null;
        for (BucketLifecycleConfiguration.Rule lcRule : rules) {
            if (lcRule.getPrefix().equals(prefix)) {
                rule = lcRule;
                break;
            }
        }
        if (rule == null && config.getBackupRetentionDays() <= 0) return false;
        if (rule != null && rule.getExpirationInDays() == config.getBackupRetentionDays()) {
            logger.info("Cleanup rule already set");
            return false;
        }
        if (rule == null) {
            // Create a new rule
            rule =
                    new BucketLifecycleConfiguration.Rule()
                            .withExpirationInDays(config.getBackupRetentionDays())
                            .withPrefix(prefix);
            rule.setStatus(BucketLifecycleConfiguration.ENABLED);
            rule.setId(prefix);
            rules.add(rule);
            logger.info(
                    "Setting cleanup for {} to {} days",
                    rule.getPrefix(),
                    rule.getExpirationInDays());
        } else if (config.getBackupRetentionDays() > 0) {
            logger.info(
                    "Setting cleanup for {} to {} days",
                    rule.getPrefix(),
                    config.getBackupRetentionDays());
            rule.setExpirationInDays(config.getBackupRetentionDays());
        } else {
            logger.info("Removing cleanup rule for {}", rule.getPrefix());
            rules.remove(rule);
        }
        return true;
    }

    void checkSuccessfulUpload(
            CompleteMultipartUploadResult resultS3MultiPartUploadComplete, Path localPath)
            throws BackupRestoreException {
        if (null != resultS3MultiPartUploadComplete
                && null != resultS3MultiPartUploadComplete.getETag()) {
            logger.info(
                    "Uploaded file: {}, object eTag: {}",
                    localPath,
                    resultS3MultiPartUploadComplete.getETag());
        } else {
            throw new BackupRestoreException(
                    "Error uploading file as ETag or CompleteMultipartUploadResult is NULL -"
                            + localPath);
        }
    }

    @Override
    public long getFileSize(Path remotePath) throws BackupRestoreException {
        return s3Client.getObjectMetadata(getShard(), remotePath.toString()).getContentLength();
    }

    @Override
    public boolean doesRemoteFileExist(Path remotePath) throws BackupRestoreException {
        boolean exists = false;
        try {
            exists = s3Client.doesObjectExist(getShard(), remotePath.toString());
        } catch (AmazonClientException ex) {
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
    public void deleteRemoteFiles(List<Path> remotePaths) throws BackupRestoreException {
        if (remotePaths.isEmpty()) return;

        try {
            List<DeleteObjectsRequest.KeyVersion> keys =
                    remotePaths
                            .stream()
                            .map(
                                    remotePath ->
                                            new DeleteObjectsRequest.KeyVersion(
                                                    remotePath.toString()))
                            .collect(Collectors.toList());
            s3Client.deleteObjects(
                    new DeleteObjectsRequest(getShard()).withKeys(keys).withQuiet(true));
            logger.info("Deleted {} objects from S3", remotePaths.size());
        } catch (Exception e) {
            logger.error("Error while trying to delete the objects from S3: {}", e.getMessage());
            throw new BackupRestoreException(e + " while trying to delete the objects");
        }
    }

    final long getChunkSize(Path localPath) throws BackupRestoreException {
        long chunkSize = config.getBackupChunkSize();
        long fileSize = localPath.toFile().length();

        // compute the size of each block we will upload to endpoint
        if (fileSize > 0)
            chunkSize =
                    (fileSize / chunkSize >= MAX_CHUNKS)
                            ? (fileSize / (MAX_CHUNKS - 1))
                            : chunkSize;

        return chunkSize;
    }
}
