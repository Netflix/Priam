package com.priam.aws;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.BackupRestoreException;
import com.priam.backup.IBackupFileSystem;
import com.priam.backup.SnappyCompression;
import com.priam.conf.IConfiguration;
import com.priam.utils.SystemUtils;

@Singleton
public class S3FileSystem implements IBackupFileSystem
{
    public static final int MIN_PART_SIZE = (6 * 1024 * 1024); // 6MB
    public static final char PATH_SEP = '/';
    private ThreadPoolExecutor executor;
    private IConfiguration config;
    private AmazonS3 s3Client;

    @Inject
    Provider<AbstractBackupPath> pathProvider;

    @Inject
    SnappyCompression compress;

    @Inject
    public S3FileSystem(ICredential provider, IConfiguration config)
    {
        AWSCredentials cred = new BasicAWSCredentials(provider.getAccessKeyId(), provider.getSecretAccessKey());
        this.executor = new ThreadPoolExecutor(0, config.getMaxBackupUploadThreads(), 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        this.s3Client = new AmazonS3Client(cred);
        this.config = config;
    }

    @Override
    public void download(AbstractBackupPath backupfile) throws BackupRestoreException
    {
        try
        {
            S3Object obj = s3Client.getObject(config.getBackupPrefix(), backupfile.getRemotePath());
            File retoreFile = backupfile.newRestoreFile();
            File tmpFile = new File(retoreFile.getAbsolutePath() + ".tmp");
            SystemUtils.copyAndClose(obj.getObjectContent(), new FileOutputStream(tmpFile));
            // Extra step: snappy seems to have boundary problems with stream
            compress.decompressAndClose(new FileInputStream(tmpFile), new FileOutputStream(retoreFile));
            tmpFile.delete();
        }
        catch (Exception e)
        {
            throw new BackupRestoreException(e.getMessage(), e);
        }
    }

    @Override
    public void upload(AbstractBackupPath backupfile) throws BackupRestoreException
    {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.getBackupPrefix(), backupfile.getRemotePath());
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart part = new DataPart(config.getBackupPrefix(), backupfile.getRemotePath(), initResponse.getUploadId());
        List<Future<PartETag>> futures = Lists.newArrayList();
        try
        {
            // FileInputStream fis = new FileInputStream(backupfile.localFile);
            Iterator<byte[]> chunks = compress.compress(backupfile.localReader());
            int partNum = 0;
            // Upload parts.
            while (chunks.hasNext())
            {
                byte[] chunk = chunks.next();
                DataPart dp = new DataPart(++partNum, chunk, config.getBackupPrefix(), backupfile.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp);
                futures.add(executor.submit(partUploader));
            }
            // sleep till we
            List<PartETag> partETags = Lists.newArrayList();
            for (Future<PartETag> future : futures)
                partETags.add(future.get());
            new S3PartUploader(s3Client, part).completeUpload(partETags);
        }
        catch (Exception e)
        {
            new S3PartUploader(s3Client, part).abortUpload();
            throw new BackupRestoreException("Error uploading file " + backupfile.fileName, e);
        }
    }

    @Override
    public int getActivecount()
    {
        return executor.getActiveCount();
    }

    @Override
    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till)
    {
        return new S3FileIterator(pathProvider, s3Client, bucket, start, till);
    }
}
