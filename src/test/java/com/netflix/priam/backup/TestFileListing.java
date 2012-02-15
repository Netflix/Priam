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
import com.netflix.priam.FakeMembership;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.identity.IMembership;

public class TestFileListing
{
    private static IConfiguration conf;
    private static ICredential cred;
    private static Injector injector;
    private static int FILECOUNT = 6;
    private static String[] snapshots = new String[2];

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        cred = injector.getInstance(ICredential.class);
        conf = injector.getInstance(IConfiguration.class);

        List<String> instances = new ArrayList<String>();
        instances.add("fakeinstance1");
        instances.add("fakeinstance2");

        FakeMembership membership = (FakeMembership) injector.getInstance(IMembership.class);
        membership.setInstances(instances);

        TestFileListing.cleanup();
        File tmpFile = new File("tmp.db");
        byte b = 8;
        long size = 100;
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(tmpFile));

        for (long i = 0; i < size; i++)
        {
            bos1.write(b);
        }
        bos1.close();

        AWSCredentials awscred = new BasicAWSCredentials(cred.getAccessKeyId(), cred.getSecretAccessKey());
        AmazonS3 s3Client = new AmazonS3Client(awscred);

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

        TestFileListing.snapshots[0] = AbstractBackupPath.DAY_FORMAT.format(cal.getTime());
        cal.add(Calendar.DATE, -1);
        TestFileListing.snapshots[1] = AbstractBackupPath.DAY_FORMAT.format(cal.getTime());
        for (String snapshot : TestFileListing.snapshots)
        {
            for (int k = 0; k < FILECOUNT; ++k)
            {

                s3Client.putObject(conf.getBackupPrefix(), "fake-app/fake_base_dir/" + snapshot + "/meta/" + (k * 10000) + "123456/fake-region/meta.json", new File("tmp.db"));
                s3Client.putObject(conf.getBackupPrefix(), "fake-app/fake_base_dir/" + snapshot + "/somefile" + k, new File("tmp.db"));
            }
            s3Client.putObject(conf.getBackupPrefix(), "fake-app/fake_base_dir/" + snapshot + "/meta/123456/some-region/meta.json", new File("tmp.db"));
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

//    @Test
//    public void testSnapshotList() throws BackupRestoreException, InterruptedException
//    {
//        S3FileListing listing = TestFileListing.injector.getInstance(S3FileListing.class);
//        String s3prefix = "fake-app/fake_base_dir/" + snapshots[0];
//        Map<String, SnapshotFileList> map = listing.getSnapshots(TestFileListing.conf.getBackupPrefix(), s3prefix);
//        Assert.assertEquals(1, map.size());
//        Assert.assertEquals(FILECOUNT + 1, map.get(snapshots[0]).size());
//    }
//
//    @Test
//    public void testSnapshotListMultiple() throws BackupRestoreException, InterruptedException
//    {
//        S3FileListing listing = TestFileListing.injector.getInstance(S3FileListing.class);
//        String s3prefix = "fake-app/fake_base_dir/" + snapshots[1].substring(0, 5);
//        Map<String, SnapshotFileList> map = listing.getSnapshots(TestFileListing.conf.getBackupPrefix(), s3prefix);
//        Assert.assertEquals(2, map.size());
//        Assert.assertEquals(FILECOUNT + 1, map.get(snapshots[0]).size());
//        Assert.assertEquals(FILECOUNT + 1, map.get(snapshots[1]).size());
//    }
//
//    @Test
//    public void testSnapshotListNone() throws BackupRestoreException, InterruptedException
//    {
//        S3FileListing listing = TestFileListing.injector.getInstance(S3FileListing.class);
//        String s3prefix = "fake-app/fake_base_dir/201106";
//        Map<String, SnapshotFileList> map = listing.getSnapshots(TestFileListing.conf.getBackupPrefix(), s3prefix);
//        Assert.assertEquals(0, map.size());
//    }
//
//    @Test
//    public void testSpecificSnapshot() throws BackupRestoreException, InterruptedException
//    {
//        S3FileListing listing = TestFileListing.injector.getInstance(S3FileListing.class);
//        RestoreRequest req = new RestoreRequest();
//        req.setBaseDir("fake-app/fake_base_dir/");
//        req.setBucketName(conf.getBackupPrefix());
//        req.setSnapshotName(snapshots[1]);
//        SnapshotFileList flist = listing.getLatestSnapshotList(req);
//
//        Assert.assertEquals(FILECOUNT + 1, flist.size());
//        Assert.assertEquals(snapshots[1], flist.getSnapshotName());
//    }
//
//    @Test
//    public void testLatestSnapshot() throws BackupRestoreException, InterruptedException
//    {
//        S3FileListing listing = TestFileListing.injector.getInstance(S3FileListing.class);
//        RestoreRequest req = new RestoreRequest();
//        req.setBaseDir("fake-app/fake_base_dir/");
//        req.setBucketName(conf.getBackupPrefix());
//
//        SnapshotFileList flist = listing.getLatestSnapshotList(req);
//
//        Assert.assertEquals(FILECOUNT + 1, flist.size());
//        Assert.assertEquals(snapshots[0], flist.getSnapshotName());
//    }

}
