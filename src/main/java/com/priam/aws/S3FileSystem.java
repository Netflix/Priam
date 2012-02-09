package com.priam.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.BackupRestoreException;
import com.priam.backup.IBackupFileSystem;
import com.priam.backup.SnappyCompression;
import com.priam.conf.IConfiguration;
import com.priam.scheduler.CustomizedThreadPoolExecutor;
import com.priam.utils.RetryableCallable;
import com.priam.utils.Throttle;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class S3FileSystem implements IBackupFileSystem, S3FileSystemMBean {
    AtomicLong bytesDownloaded = new AtomicLong();
    AtomicLong bytesUploaded = new AtomicLong();
    AtomicInteger uploadCount = new AtomicInteger();
    AtomicInteger downloadCount = new AtomicInteger();

    // 6MB
    public static final int MIN_PART_SIZE = (6 * 1024 * 1024);
    // timeout is set to 2 hours.
    private static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);
    public static final char PATH_SEP = '/';
    private CustomizedThreadPoolExecutor executor;
    private IConfiguration config;
    private AmazonS3 s3Client;

    @Inject
    Provider<AbstractBackupPath> pathProvider;

    @Inject
    SnappyCompression compress;

    Throttle throttle;

    @Inject
    public S3FileSystem(AWSCredentials credentials, final IConfiguration config) {
        int threads = config.getMaxBackupUploadThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(threads);
        this.executor = new CustomizedThreadPoolExecutor(threads, queue, UPLOAD_TIMEOUT);
        this.s3Client = new AmazonS3Client(credentials);
        this.config = config;
        this.throttle = new Throttle(this.getClass().getCanonicalName(), new Throttle.ThroughputFunction() {
            public int targetThroughput() {
                int throttleLimit = config.getUploadThrottle();
                if (throttleLimit < 1)
                    return 0;
                int totalBytesPerMS = (throttleLimit * 1024 * 1024) / 1000;
                return totalBytesPerMS;
            }
        });
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = MBEAN_NAME;
        try {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void download(AbstractBackupPath backupfile) throws BackupRestoreException {
        try {
            new S3FileDownloader(backupfile).call();
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage(), e);
        }
    }

    @Override
    public void download(AbstractBackupPath backupfile, OutputStream os) throws BackupRestoreException {
        try {
            new S3FileDownloader(backupfile, os).call();
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage(), e);
        }
    }

    @Override
    public void upload(AbstractBackupPath backupfile) throws BackupRestoreException {
        try {
            new S3FileUploader(backupfile).call();
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading file " + backupfile.fileName, e);
        }
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException {
        try {
            new S3FileUploader(path, in).call();
        } catch (Exception e) {
            throw new BackupRestoreException("Error uploading stream " + path.fileName, e);
        }
    }

    @Override
    public int getActivecount() {
        return executor.getActiveCount();
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
        return new S3FileIterator(pathProvider, s3Client, path, start, till);
    }

    public class S3FileUploader extends RetryableCallable<Void> {
        private AbstractBackupPath backupfile;
        private InputStream is;

        public S3FileUploader(AbstractBackupPath backupfile) {
            this.backupfile = backupfile;
            uploadCount.incrementAndGet();
        }

        public S3FileUploader(AbstractBackupPath backupfile, InputStream is) {
            super(1, 0);// no retries
            this.backupfile = backupfile;
            this.is = is;
            uploadCount.incrementAndGet();
        }

        @Override
        public Void retriableCall() throws Exception {
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), backupfile.getRemotePath());
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
            DataPart part = new DataPart(config.getBackupPrefix(), backupfile.getRemotePath(), initResponse.getUploadId());
            List<PartETag> partETags = Lists.newArrayList();
            try {
                Iterator<byte[]> chunks;
                if (is == null)
                    chunks = compress.compress(backupfile.localReader(), config.getBackupChunkSize());
                else
                    chunks = compress.compress(is, config.getBackupChunkSize());
                // Upload parts.
                int partNum = 0;
                while (chunks.hasNext()) {
                    byte[] chunk = chunks.next();
                    throttle.throttle(chunk.length);
                    DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), backupfile.getRemotePath(), initResponse.getUploadId());
                    S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                    executor.submit(partUploader);
                    bytesUploaded.addAndGet(chunk.length);
                }
                executor.sleepTillEmpty();
                if (partNum != partETags.size())
                    throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the uploaded parts(" + partETags.size() + ")");
                new S3PartUploader(s3Client, part, partETags).completeUpload();
            } catch (Exception e) {
                new S3PartUploader(s3Client, part, partETags).abortUpload();
                throw new BackupRestoreException("Error uploading file " + backupfile.fileName, e);
            }
            return null;
        }
    }

    public class S3FileDownloader extends RetryableCallable<Void> {
        private AbstractBackupPath backupfile;
        private OutputStream os;

        public S3FileDownloader(AbstractBackupPath backupfile) {
            this.backupfile = backupfile;
            downloadCount.incrementAndGet();
        }

        public S3FileDownloader(AbstractBackupPath backupfile, OutputStream os) {
            this.backupfile = backupfile;
            downloadCount.incrementAndGet();
            this.os = os;
        }

        @Override
        public Void retriableCall() throws Exception {
            S3Object obj = s3Client.getObject(getPrefix(), backupfile.getRemotePath());
            if (os == null) {
                File retoreFile = backupfile.newRestoreFile();
                compress.decompressAndClose(obj.getObjectContent(), new FileOutputStream(retoreFile));
            } else {
                compress.decompressAndClose(obj.getObjectContent(), os);
            }
            bytesDownloaded.addAndGet(obj.getObjectMetadata().getContentLength());
            return null;
        }
    }

    public String getPrefix() {
        String prefix = "";
        if (!"".equals(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();

        String[] paths = prefix.split(String.valueOf(S3BackupPath.PATH_SEP));
        return paths[0];
    }

    @Override
    public int downloadCount() {
        return downloadCount.get();
    }

    @Override
    public int uploadCount() {
        return uploadCount.get();
    }

    @Override
    public long bytesUploaded() {
        return bytesUploaded.get();
    }

    @Override
    public long bytesDownloaded() {
        return bytesDownloaded.get();
    }

}
