package com.netflix.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;

public class TestIncrementalRestore
{
    private static IConfiguration conf;
    private static ICredential cred;
    private static Injector injector;
    private static int FILECOUNT = 10;
    private static List<String> remoteFiles;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {

        injector = Guice.createInjector(new BRTestModule());
        cred = injector.getInstance(ICredential.class);
        conf = injector.getInstance(IConfiguration.class);
        TestIncrementalRestore.cleanup();
        TestIncrementalRestore.remoteFiles = new ArrayList<String>();
        File tmpFile = new File("tmp.db");
        byte b = 8;
        long size = (2L * 1024);
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));

        for (long i = 0; i < size; i++)
        {
            bos1.write(b);
        }
        bos1.close();

        AWSCredentials awscred = new BasicAWSCredentials(cred.getAccessKeyId(), cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.add(Calendar.DATE, -2);
        for (int k = 0; k < FILECOUNT; ++k)
        {
            String remoteFilePath = "fake-app/fake_base_dir/" + AbstractBackupPath.DAY_FORMAT.format(cal.getTime()) + "/sst/123456/fake-region/ks1/";
            s3Client.putObject(conf.getBackupPrefix(), remoteFilePath + k + ".db", new File("tmp.db"));
            cal.add(Calendar.HOUR, 6);
            remoteFiles.add(remoteFilePath);
        }
        for (int k = 0; k < FILECOUNT / 2; ++k)
        {
            String remoteFilePath = "fake-app/fake_base_dir/" + AbstractBackupPath.DAY_FORMAT.format(cal.getTime()) + "/snp/123456/fake-region/ks1/";
            s3Client.putObject(conf.getBackupPrefix(), remoteFilePath + k + ".db", new File("tmp.db"));
            cal.add(Calendar.HOUR, 6);
            remoteFiles.add(remoteFilePath);
        }

    }

    @AfterClass
    public static void cleanup() throws InterruptedException, IOException
    {
        File tmpFile = new File("tmp.db");
        if (tmpFile.exists())
            tmpFile.delete();

        AWSCredentials awscred = new BasicAWSCredentials(cred.getAccessKeyId(), cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);
        ListObjectsRequest listReq = new ListObjectsRequest();
        listReq.setBucketName(conf.getBackupPrefix());
        listReq.setPrefix("fake-app");

        ObjectListing objectListing = s3Client.listObjects(listReq);
        do
        {
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries())
            {
                s3Client.deleteObject(conf.getBackupPrefix(), objectSummary.getKey());
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } while (objectListing.isTruncated() || objectListing.getObjectSummaries().size() > 0);
    }

    // @Test
    // public void testIncrementalDownload() throws BackupRestoreException,
    // InterruptedException
    // {
    //
    // S3IncrementalRestore restore =
    // TestIncrementalRestore.injector.getInstance(S3IncrementalRestore.class);
    // RestoreRequest req = new RestoreRequest();
    // Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    // cal.add(Calendar.MINUTE, 10);
    // cal.add(Calendar.DATE, -1);
    // req.setSnapshotName(BackupPath.dayformat.format(cal.getTime()));
    // req.setBucketName("TEST-netflix.platform.S3");
    // req.setBaseDir("fake-app/fake_base_dir/");
    // req.setToken("123456");
    // restore.setRestoreRequest(req);
    // restore.restore();
    // Assert.assertEquals(4, restore.getResultList().size());
    // }

}
