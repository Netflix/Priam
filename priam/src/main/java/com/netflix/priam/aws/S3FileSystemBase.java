/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Provider;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IBackupMetrics;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.merics.AWSSlowDownExceptionMeasurement;
import com.netflix.priam.merics.BackupUploadRateMeasurement;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.notification.BackupEvent;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.EventGenerator;
import com.netflix.priam.notification.EventObserver;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class S3FileSystemBase implements IBackupFileSystem, EventGenerator<BackupEvent> {
    protected static final int MAX_CHUNKS = 10000;
    protected static final long MAX_BUFFERED_IN_STREAM_SIZE = 5 * 1024 * 1024;
    protected static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystemBase.class);
    protected AtomicInteger uploadCount = new AtomicInteger();
    protected AtomicLong bytesUploaded = new AtomicLong(); //bytes uploaded per file
    protected AtomicInteger downloadCount = new AtomicInteger();
    protected AtomicLong bytesDownloaded = new AtomicLong();

    protected IMetricPublisher metricPublisher;
    protected IMeasurement awsSlowDownMeasurement;
    protected int awsSlowDownExceptionCounter = 0;

    protected AmazonS3 s3Client;
    protected IConfiguration config;
    protected Provider<AbstractBackupPath> pathProvider;
    protected ICompression compress;
    protected IBackupMetrics backupMetricsMgr;
    protected BlockingSubmitThreadPoolExecutor executor;
    protected RateLimiter rateLimiter; //a throttling mechanism, we can limit the amount of bytes uploaded to endpoint per second.
    private List<EventObserver> observers = new ArrayList<>();
    private final Object MUTEX = new Object();


    public S3FileSystemBase(Provider<AbstractBackupPath> pathProvider,
                            ICompression compress,
                            final IConfiguration config,
                            IMetricPublisher metricPublisher,
                            IBackupMetrics backupMetricsMgr,
                            BackupNotificationMgr backupNotificationMgr) {
        this.pathProvider = pathProvider;
        this.compress = compress;
        this.config = config;
        this.metricPublisher = metricPublisher;
        this.backupMetricsMgr = backupMetricsMgr;
        awsSlowDownMeasurement = new AWSSlowDownExceptionMeasurement(); //a counter of AWS warning for all uploads


        int threads = config.getMaxBackupUploadThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(threads);
        this.executor = new BlockingSubmitThreadPoolExecutor(threads, queue, UPLOAD_TIMEOUT);

        double throttleLimit = config.getUploadThrottle();
        this.rateLimiter = RateLimiter.create(throttleLimit < 1 ? Double.MAX_VALUE : throttleLimit);
        this.addObserver(backupNotificationMgr);
    }


    public AmazonS3 getS3Client() {
        return s3Client;
    }

    /*
     * A means to change the default handle to the S3 client.
     */
    public void setS3Client(AmazonS3 client) {
        s3Client = client;
    }

    /**
     * Get S3 prefix which will be used to locate S3 files
     */
    protected String getPrefix(IConfiguration config) {
        String prefix;
        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();

        String[] paths = prefix.split(String.valueOf(S3BackupPath.PATH_SEP));
        return paths[0];
    }

    @Override
    public void cleanup() {

        AmazonS3 s3Client = getS3Client();
        String clusterPath = pathProvider.get().clusterPrefix("");
        logger.debug("Bucket: {}", config.getBackupPrefix());
        BucketLifecycleConfiguration lifeConfig = s3Client.getBucketLifecycleConfiguration(config.getBackupPrefix());
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
            } else
                s3Client.deleteBucketLifecycleConfiguration(config.getBackupPrefix());
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
        if (rule == null && config.getBackupRetentionDays() <= 0)
            return false;
        if (rule != null && rule.getExpirationInDays() == config.getBackupRetentionDays()) {
            logger.info("Cleanup rule already set");
            return false;
        }
        if (rule == null) {
            // Create a new rule
            rule = new BucketLifecycleConfiguration.Rule().withExpirationInDays(config.getBackupRetentionDays()).withPrefix(prefix);
            rule.setStatus(BucketLifecycleConfiguration.ENABLED);
            rule.setId(prefix);
            rules.add(rule);
            logger.info("Setting cleanup for {} to {} days", rule.getPrefix(), rule.getExpirationInDays());
        } else if (config.getBackupRetentionDays() > 0) {
            logger.info("Setting cleanup for {} to {} days", rule.getPrefix(), config.getBackupRetentionDays());
            rule.setExpirationInDays(config.getBackupRetentionDays());
        } else {
            logger.info("Removing cleanup rule for {}", rule.getPrefix());
            rules.remove(rule);
        }
        return true;
    }

    /*
    @param path - representation of the file uploaded
    @param start time of upload, in millisecs
    @param completion time of upload, in millsecs
     */
    protected void postProcessingPerFile(AbstractBackupPath path, long startTimeInMilliSecs, long completedTimeInMilliSecs) {
        //Publish upload rate for each uploaded file
        try {
            long sizeInBytes = path.getSize();
            long elapseTimeInMillisecs = completedTimeInMilliSecs - startTimeInMilliSecs;
            long elapseTimeInSecs = elapseTimeInMillisecs / 1000; //converting millis to seconds as 1000m in 1 second
            long bytesReadPerSec = 0;
            Double speedInKBps = 0.0;
            if (elapseTimeInSecs > 0 && sizeInBytes > 0) {
                bytesReadPerSec = sizeInBytes / elapseTimeInSecs;
                speedInKBps = bytesReadPerSec / 1024D;
            } else {
                bytesReadPerSec = sizeInBytes;  //we uploaded the whole file in less than a sec
                speedInKBps = (double) sizeInBytes;
            }

            logger.info("Upload rate for file: {}"
                    + ", elapsse time in sec(s): {}"
                    + ", KB per sec: {}",
                    path.getFileName(), elapseTimeInSecs, speedInKBps);

            /*
            This measurement is different than most others.  Other measurements are applicable to all occurrences (e.g
            node tool flush errors, AWS TPS warning errors).  Upload rate for all occurrences (uploads) is not useful; rather,
            we are interested in the upload rate per file.  Hence "metadata" is the upload rate for the just uploaded file.
             */
            IMeasurement backupUploadRateMeasurement = new BackupUploadRateMeasurement();
            BackupUploadRateMeasurement.Metadata metadata = new BackupUploadRateMeasurement.Metadata(path.getFileName(), speedInKBps, elapseTimeInMillisecs);
            backupUploadRateMeasurement.setVal(metadata);
            this.metricPublisher.publish(backupUploadRateMeasurement); //signal of upload rate for file

            awsSlowDownMeasurement.incrementFailureCnt(path.getAWSSlowDownExceptionCounter());
            this.metricPublisher.publish(awsSlowDownMeasurement); //signal of possible throttling by aws

        } catch (Exception e) {
            logger.error("Post processing of file {} failed, not fatal.", path.getFileName(), e);
        }
    }

    /*
    Reinitializtion which should be performed before uploading a file
     */
    protected void reinitialize() {
        bytesUploaded = new AtomicLong(0); //initialize
        this.awsSlowDownExceptionCounter = 0;
    }

    /*
    @param file uploaded to S3
    @param a list of unique parts uploaded to S3 for file
     */
    protected void logDiagnosticInfo(AbstractBackupPath fileUploaded, CompleteMultipartUploadResult res) {
        File f = fileUploaded.getBackupFile();
        String fName = f.getAbsolutePath();
        logger.info("Uploaded file: {}, object eTag: {}", fName, res.getETag());
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException {
        reinitialize();  //perform before file upload
        uploadCount.incrementAndGet();
        long chunkSize = config.getBackupChunkSize();
        if (path.getSize() > 0)
            chunkSize = (path.getSize() / chunkSize >= MAX_CHUNKS) ? (path.getSize() / (MAX_CHUNKS - 1)) : chunkSize; //compute the size of each block we will upload to endpoint

        logger.info("Uploading to {}/{} with chunk size {}", config.getBackupPrefix(), path.getRemotePath(), chunkSize);
        long startTime = System.nanoTime(); //initialize for each file upload
        notifyEventStart(new BackupEvent(path));

        uploadFile(path, in, chunkSize);
        long completedTime = System.nanoTime();
        postProcessingPerFile(path, TimeUnit.NANOSECONDS.toMillis(startTime), TimeUnit.NANOSECONDS.toMillis(completedTime));
        notifyEventSuccess(new BackupEvent(path));
    }

    protected void checkSuccessfulUpload(CompleteMultipartUploadResult resultS3MultiPartUploadComplete, AbstractBackupPath path) throws BackupRestoreException {
        if (null != resultS3MultiPartUploadComplete && null != resultS3MultiPartUploadComplete.getETag()) {
            String eTagObjectId = resultS3MultiPartUploadComplete.getETag(); //unique id of the whole object
            logDiagnosticInfo(path, resultS3MultiPartUploadComplete);
        } else {
            this.backupMetricsMgr.incrementInvalidUploads();
            throw new BackupRestoreException("Error uploading file as ETag or CompleteMultipartUploadResult is NULL -" + path.getFileName());
        }
    }

    protected BackupRestoreException encounterError(AbstractBackupPath path, S3PartUploader s3PartUploader, Exception e) {
        this.backupMetricsMgr.incrementInvalidUploads();
        if (e instanceof AmazonS3Exception) {
            AmazonS3Exception a = (AmazonS3Exception) e;
            String amazoneErrorCode = a.getErrorCode();
            if (amazoneErrorCode != null && !amazoneErrorCode.isEmpty()) {
                if (amazoneErrorCode.equalsIgnoreCase("slowdown")) {
                    awsSlowDownExceptionCounter += 1;
                    logger.warn("Received slow down from AWS when uploading file: {}", path.getFileName());
                }
            }
        }
        logger.error("Error uploading file {}, a datapart was not uploaded.", path.getFileName(), e);
        s3PartUploader.abortUpload();
        notifyEventFailure(new BackupEvent(path));
        return new BackupRestoreException("Error uploading file " + path.getFileName(), e);
    }

    abstract void uploadFile(AbstractBackupPath path, InputStream in, long chunkSize) throws BackupRestoreException;

    /**
     * This method does exactly as other download method.(Supposed to be overridden)
     * filePath parameter provides the diskPath of the downloaded file.
     * This path can be used to correlate the files which are Streamed In
     * during Incremental Restores
     */
    @Override
    public void download(AbstractBackupPath path, OutputStream os,
                         String filePath) throws BackupRestoreException {
        try {
            // Calling original Download method
            download(path, os);
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage(), e);
        }

    }

    @Override
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {
        logger.info("Downloading {} from S3 bucket {}", path.getRemotePath(), getPrefix(this.config));
        downloadCount.incrementAndGet();
        long contentLen = s3Client.getObjectMetadata(getPrefix(config), path.getRemotePath()).getContentLength();
        path.setSize(contentLen);
        downloadFile(path, os);
        bytesDownloaded.addAndGet(contentLen);
    }

    protected abstract void downloadFile(AbstractBackupPath path, OutputStream os) throws BackupRestoreException;

    @Override
    public long getBytesUploaded() {
        return bytesUploaded.get();
    }

    @Override
    public int getAWSSlowDownExceptionCounter() {
        return awsSlowDownExceptionCounter;
    }

    @Override
    public void shutdown() {
        if (executor != null)
            executor.shutdown();

    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date) {
        return new S3PrefixIterator(config, pathProvider, s3Client, date);
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
        return new S3FileIterator(pathProvider, s3Client, path, start, till);
    }


    @Override
    public void addObserver(EventObserver observer) {
        if (observers == null)
            observers = new ArrayList<>();

        synchronized (MUTEX) {
            if (!observers.contains(observer))
                observers.add(observer);
        }
    }

    @Override
    public void removeObserver(EventObserver observer) {
        if (observers == null || observers.isEmpty())
            return;
        synchronized (MUTEX) {
            observers.remove(observer);
        }
    }

    @Override
    public void notifyEventStart(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventStart(event));
    }

    @Override
    public void notifyEventSuccess(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventSuccess(event));
    }

    @Override
    public void notifyEventFailure(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventFailure(event));
    }

    @Override
    public void notifyEventStop(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventStop(event));
    }

    private boolean shouldNotifyObservers() {
        return (observers != null && !observers.isEmpty());
    }

}