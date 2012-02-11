package com.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;
import mockit.Mock;
import mockit.Mockit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.DataPart;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.S3PartUploader;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.utils.RetryableCallable;

public class TestS3FileSystem
{
    private static Injector injector;
    private static final Logger logger = LoggerFactory.getLogger(TestBackup.class);
    private static String FILE_PATH = "cass/data/ks1/snapshots/201108082320/f1.db";

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        Mockit.setUpMock(S3PartUploader.class, MockS3PartUploader.class);
        Mockit.setUpMock(AmazonS3Client.class, MockAmazonS3Client.class);
        injector = Guice.createInjector(new BRTestModule());

        File dir1 = new File("cass/data/ks1/snapshots/201108082320");
        if (!dir1.exists())
            dir1.mkdirs();
        File file = new File(FILE_PATH);
        long fiveKB = (5L * 1024);
        byte b = 8;
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
        for (long i = 0; i < fiveKB; i++)
        {
            bos1.write(b);
        }
        bos1.close();
    }

    @AfterClass
    public static void cleanup()
    {
        File file = new File(FILE_PATH);
        file.delete();
    }

    @Test
    public void testFileUpload() throws Exception
    {
        MockS3PartUploader.setup();
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        String snapshotfile = "cass/data/ks1/snapshots/201108082320/f1.db";
        S3BackupPath backupfile = injector.getInstance(S3BackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        fs.upload(backupfile);
        Assert.assertEquals(1, MockS3PartUploader.compattempts);
    }

    @Test
    public void testFileUploadFailures() throws Exception
    {
        MockS3PartUploader.setup();
        MockS3PartUploader.partFailure = true;
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        String snapshotfile = "cass/data/ks1/snapshots/201108082320/f1.db";
        S3BackupPath backupfile = injector.getInstance(S3BackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        try
        {
            fs.upload(backupfile);
        }
        catch (BackupRestoreException e)
        {
            //ignore
        }
        //Actual retries = S3PartUploader.MAX_RETRIES*RetryableCallable.DEFAULT_NUMBER_OF_RETRIES
        Assert.assertEquals(RetryableCallable.DEFAULT_NUMBER_OF_RETRIES*RetryableCallable.DEFAULT_NUMBER_OF_RETRIES, MockS3PartUploader.partAttempts);
        Assert.assertEquals(0, MockS3PartUploader.compattempts);        
    }

    @Test
    public void testFileUploadCompleteFailure() throws Exception
    {
        MockS3PartUploader.setup();
        MockS3PartUploader.completionFailure = true;
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        String snapshotfile = "cass/data/ks1/snapshots/201108082320/f1.db";
        S3BackupPath backupfile = injector.getInstance(S3BackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        try
        {
            fs.upload(backupfile);
        }
        catch (BackupRestoreException e)
        {
            //ignore
        }
        //Actual retries = S3PartUploader.MAX_RETRIES*RetryableCallable.DEFAULT_NUMBER_OF_RETRIES
        Assert.assertEquals(RetryableCallable.DEFAULT_NUMBER_OF_RETRIES, MockS3PartUploader.partAttempts);
        Assert.assertEquals(RetryableCallable.DEFAULT_NUMBER_OF_RETRIES, MockS3PartUploader.compattempts);        
    }

    // Mock Nodeprobe class
    @Ignore
    public static class MockS3PartUploader extends RetryableCallable<Void>
    {
        public static int compattempts = 0;
        public static int partAttempts = 0;
        public static boolean partFailure = false;
        public static boolean completionFailure = false;
        private static List<PartETag> partETags;
        
        @Mock
        public void $init(AmazonS3 client, DataPart dp, List<PartETag> partETags)
        {       
            this.partETags = partETags;
        }

        @Mock
        private Void uploadPart() throws AmazonClientException, BackupRestoreException
        {
            ++partAttempts;
            if(partFailure)
                throw new BackupRestoreException("Test exception");
            this.partETags.add(new PartETag(0, null));
            return null;
        }

        @Mock
        public void completeUpload() throws BackupRestoreException
        {
            ++compattempts;
            if(completionFailure){
                System.out.println("comepletion " + compattempts);
                throw new BackupRestoreException("Test exception");
                
            }
        }

        @Mock
        public void abortUpload()
        {
        }

        @Override
        @Mock
        public Void retriableCall() throws AmazonClientException, BackupRestoreException
        {
            logger.info("MOCK UPLOADING...");
            return uploadPart();
        }
        
        public static void setup(){
            compattempts = 0;
            partAttempts = 0;
            partFailure = false;
            completionFailure = false;            
        }
    }

    @Ignore
    public static class MockAmazonS3Client
    {
        @Mock
        public void $init()
        {
        }

        @Mock
        public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest initiateMultipartUploadRequest) throws AmazonClientException, AmazonServiceException
        {
            return new InitiateMultipartUploadResult();
        }
    }
}
