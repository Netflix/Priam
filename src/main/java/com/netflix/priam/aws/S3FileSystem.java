package com.netflix.priam.aws;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
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
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.scheduler.CustomizedThreadPoolExecutor;
import com.netflix.priam.utils.Throttle;

/**
 * Implementation of IBackupFileSystem for S3
 */
@Singleton
public class S3FileSystem implements IBackupFileSystem, S3FileSystemMBean
{
    private static final int MAX_CHUNKS = 10000;
    private static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);

    private final AmazonS3 s3Client;
    private final Provider<AbstractBackupPath> pathProvider;
    private final ICompression compress;
    private final IConfiguration config;
    private Throttle throttle;
    private CustomizedThreadPoolExecutor executor;

    private AtomicLong bytesDownloaded = new AtomicLong();
    private AtomicLong bytesUploaded = new AtomicLong();
    private AtomicInteger uploadCount = new AtomicInteger();
    private AtomicInteger downloadCount = new AtomicInteger();

    @Inject
    public S3FileSystem(Provider<AbstractBackupPath> pathProvider, ICompression compress, final IConfiguration config, ICredential provider)
    {
        this.pathProvider = pathProvider;
        this.compress = compress;
        this.config = config;
        AWSCredentials cred = new BasicAWSCredentials(provider.getAccessKeyId(), provider.getSecretAccessKey());
        int threads = config.getMaxBackupUploadThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(threads);
        this.executor = new CustomizedThreadPoolExecutor(threads, queue, UPLOAD_TIMEOUT);
        this.s3Client = new AmazonS3Client(cred);
        this.throttle = new Throttle(this.getClass().getCanonicalName(), new Throttle.ThroughputFunction()
        {
            public int targetThroughput()
            {
                int throttleLimit = config.getUploadThrottle();
                if (throttleLimit < 1)
                    return 0;
                int totalBytesPerMS = (throttleLimit * 1024 * 1024) / 1000;
                return totalBytesPerMS;
            }
        });
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = MBEAN_NAME;
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void download(AbstractBackupPath backupfile, OutputStream os) throws BackupRestoreException
    {
        try
        {
            downloadCount.incrementAndGet();
            S3Object obj = s3Client.getObject(getPrefix(), backupfile.getRemotePath());
            compress.decompressAndClose(obj.getObjectContent(), os);
            bytesDownloaded.addAndGet(obj.getObjectMetadata().getContentLength());
        }
        catch (Exception e)
        {
            throw new BackupRestoreException(e.getMessage(), e);
        }
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException
    {
        uploadCount.incrementAndGet();
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), path.getRemotePath());
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart part = new DataPart(config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
        List<PartETag> partETags = Lists.newArrayList();
        long chunkSize = config.getBackupChunkSize();
        if (path.getSize() > 0)
            chunkSize = (path.getSize() / chunkSize >= MAX_CHUNKS) ? (path.getSize() / (MAX_CHUNKS - 1)) : chunkSize;
        try
        {
            Iterator<byte[]> chunks = compress.compress(in, chunkSize);
            // Upload parts.
            int partNum = 0;
            while (chunks.hasNext())
            {
                byte[] chunk = chunks.next();
                throttle.throttle(chunk.length);
                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), path.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                executor.submit(partUploader);
                bytesUploaded.addAndGet(chunk.length);
            }
            executor.sleepTillEmpty();
            if (partNum != partETags.size())
                throw new BackupRestoreException("Number of parts(" + partNum + ")  does not match the uploaded parts(" + partETags.size() + ")");
            new S3PartUploader(s3Client, part, partETags).completeUpload();
        }
        catch (Exception e)
        {
            new S3PartUploader(s3Client, part, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file " + path.getFileName(), e);
        }
    }

    @Override
    public int getActivecount()
    {
        return executor.getActiveCount();
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till)
    {
        return new S3FileIterator(pathProvider, s3Client, path, start, till);
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date)
    {
        return new S3PrefixIterator(config, pathProvider, s3Client, date);
    }

    /**
     * Get S3 prefix which will be used to locate S3 files
     */
    public String getPrefix()
    {
        String prefix = "";
        if (!"".equals(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();

        String[] paths = prefix.split(String.valueOf(S3BackupPath.PATH_SEP));
        return paths[0];
    }

    @Override
    public int downloadCount()
    {
        return downloadCount.get();
    }

    @Override
    public int uploadCount()
    {
        return uploadCount.get();
    }

    @Override
    public long bytesUploaded()
    {
        return bytesUploaded.get();
    }

    @Override
    public long bytesDownloaded()
    {
        return bytesDownloaded.get();
    }
}
