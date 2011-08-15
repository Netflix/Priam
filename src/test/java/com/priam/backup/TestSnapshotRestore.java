package com.priam.backup;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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
import com.priam.TestModule;
import com.priam.aws.ICredential;
import com.priam.conf.IConfiguration;

public class TestSnapshotRestore
{
    private static IConfiguration conf;
    private static ICredential cred;
    private static String REMOTE_FILE_PATH = "fake-app/fake_base_dir/201106281200/snp/123456/fake-region/ks1/";
    private static Injector injector;
    private static int FILECOUNT = 10;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new TestModule());
        cred = injector.getInstance(ICredential.class);
        conf = injector.getInstance(IConfiguration.class);

        File tmpFile = new File("tmp.db");
        byte b = 8;
        long size = (5L * 1024);
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));

        for (long i = 0; i < size; i++)
        {
            bos1.write(b);
        }
        bos1.close();

        AWSCredentials awscred = new BasicAWSCredentials(cred.getAccessKeyId(), cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);

        for (int k = 0; k < FILECOUNT; ++k)
        {
            s3Client.putObject(conf.getBackupPrefix(), REMOTE_FILE_PATH + k + ".db", new File("tmp.db"));
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
    // public void testSnapshotDownload() throws BackupRestoreException,
    // InterruptedException
    // {
    //
    // S3SnapshotRestore restore =
    // TestSnapshotRestore.injector.getInstance(S3SnapshotRestore.class);
    // RestoreRequest req = new RestoreRequest();
    // req.setSnapshotName("201106281200");
    // req.setBucketName("TEST-netflix.platform.S3");
    // req.setBaseDir("fake-app/fake_base_dir/");
    // req.setToken("123456");
    // restore.setRestoreRequest(req);
    // restore.restore();
    // // Assert.assertEquals(FILECOUNT, restore.getResultList().size());
    // }

}
