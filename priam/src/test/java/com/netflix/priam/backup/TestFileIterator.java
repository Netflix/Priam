package com.netflix.priam.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mockit.Mock;
import mockit.Mocked;
import mockit.Mockit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.S3FileIterator;
import com.netflix.priam.identity.InstanceIdentity;

/**
 * Unit test for backup file iterator
 * 
 * @author Praveen Sadhu
 *
 */
public class TestFileIterator
{
    private static Injector injector;
    private static Date startTime, endTime;
    private static Calendar cal;

    @Mocked
    private static AmazonS3Client s3client;
    
    private static IConfiguration conf;
    private static InstanceIdentity factory;
    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        injector = Guice.createInjector(new BRTestModule());
        conf = injector.getInstance(IConfiguration.class);
        factory = injector.getInstance(InstanceIdentity.class);

        Mockit.setUpMock(AmazonS3Client.class, MockAmazonS3Client.class);
        Mockit.setUpMock(ObjectListing.class, MockObjectListing.class);
        s3client = new AmazonS3Client();

        cal = Calendar.getInstance();
        cal.set(2011, 7, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        endTime = cal.getTime();
    }

    // MockAmazonS3Client class
    @Ignore
    public static class MockAmazonS3Client
    {
        public static String bucketName = "";
        public static String prefix = "";
        
        @Mock
        public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws AmazonClientException, AmazonServiceException
        {
            ObjectListing listing = new ObjectListing();
            listing.setBucketName(listObjectsRequest.getBucketName());
            listing.setPrefix(listObjectsRequest.getPrefix());
            return listing;
        }

        public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws AmazonClientException, AmazonServiceException
        {
            ObjectListing listing = new ObjectListing();
            listing.setBucketName(previousObjectListing.getBucketName());
            listing.setPrefix(previousObjectListing.getPrefix());
            return new ObjectListing();
        }
    }

    // MockObjectListing class
    @Ignore
    public static class MockObjectListing
    {
        public static boolean truncated = true;
        public static boolean firstcall = true;
        public static boolean simfilter = false;//Simulate filtering

        @Mock
        public List<S3ObjectSummary> getObjectSummaries()
        {
            if (firstcall)
            {   
                firstcall = false;
                if( simfilter )
                    return getObjectSummaryEmpty();                    
                return getObjectSummary();
            }
            else
            {
                if( simfilter ){
                    simfilter = false;//reset
                    return getObjectSummaryEmpty();
                }
                else
                    truncated = false;
                return getNextObjectSummary();
            }
        }

        @Mock
        public boolean isTruncated()
        {
            return truncated;
        }
    }

    @Test
    public void testIteratorEmptySet()
    {
        cal.set(2011, 7, 11, 6, 1, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date stime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        Date etime = cal.getTime();
        MockAmazonS3Client.bucketName = "TESTBUCKET";
        MockAmazonS3Client.prefix = conf.getBackupLocation() + "/" + conf.getDC() + "/" + conf.getAppName() + "/" + factory.getInstance().getToken();
        MockAmazonS3Client.prefix += "/20110811";
        
        S3FileIterator fileIterator = new S3FileIterator(injector.getProvider(AbstractBackupPath.class), s3client, "TESTBUCKET", stime, etime);
        Set<String> files = new HashSet<String>();
        while (fileIterator.hasNext())
            files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(0, files.size());
    }

    @Test
    public void testIterator()
    {
        MockObjectListing.truncated = false;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = false;
        MockAmazonS3Client.bucketName = "TESTBUCKET";
        MockAmazonS3Client.prefix = conf.getBackupLocation() + "/" + conf.getDC() + "/" + conf.getAppName() + "/" + factory.getInstance().getToken();
        MockAmazonS3Client.prefix += "/20110811";
        
        S3FileIterator fileIterator = new S3FileIterator(injector.getProvider(AbstractBackupPath.class), s3client, "TESTBUCKET", startTime, endTime);
        Set<String> files = new HashSet<String>();
        while (fileIterator.hasNext())
            files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(3, files.size());
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/META/meta.json"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db"));
    }

    @Test
    public void testIteratorTruncated()
    {
        MockObjectListing.truncated = true;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = false;
        MockAmazonS3Client.bucketName = "TESTBUCKET";
        MockAmazonS3Client.prefix = conf.getBackupLocation() + "/" + conf.getDC() + "/" + conf.getAppName() + "/" + factory.getInstance().getToken();
        MockAmazonS3Client.prefix += "/20110811";

        S3FileIterator fileIterator = new S3FileIterator(injector.getProvider(AbstractBackupPath.class), s3client, "TESTBUCKET", startTime, endTime);
        Set<String> files = new HashSet<String>();
        while (fileIterator.hasNext())
            files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(5, files.size());
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/META/meta.json"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db"));

        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db"));

    }

    @Test
    public void testIteratorTruncatedOOR()
    {
        MockObjectListing.truncated = true;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = true;
        MockAmazonS3Client.bucketName = "TESTBUCKET";
        MockAmazonS3Client.prefix = conf.getBackupLocation() + "/" + conf.getDC() + "/" + conf.getAppName() + "/" + factory.getInstance().getToken();
        MockAmazonS3Client.prefix += "/20110811";

        S3FileIterator fileIterator = new S3FileIterator(injector.getProvider(AbstractBackupPath.class), s3client, "TESTBUCKET", startTime, endTime);
        Set<String> files = new HashSet<String>();
        while (fileIterator.hasNext())
            files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(2, files.size());
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201107110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201107110430/SST/ks1/cf1/f2.db"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201107110030/META/meta.json"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201107110600/SST/ks1/cf1/f3.db"));

        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db"));
        
    }

    @Test
    public void testRestorePathIteration()
    {
        MockObjectListing.truncated = true;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = false;
        MockAmazonS3Client.bucketName = "RESTOREBUCKET";
        MockAmazonS3Client.prefix = "test_restore_backup/fake-restore-region/fakerestorecluster" + "/" + factory.getInstance().getToken();
        MockAmazonS3Client.prefix += "/20110811";

        S3FileIterator fileIterator = new S3FileIterator(injector.getProvider(AbstractBackupPath.class), s3client, "RESTOREBUCKET/test_restore_backup/fake-restore-region/fakerestorecluster", startTime, endTime);
        Set<String> files = new HashSet<String>();
        while (fileIterator.hasNext())
            files.add(fileIterator.next().getRemotePath());
        while (fileIterator.hasNext())
            files.add(fileIterator.next().getRemotePath());

        Assert.assertEquals(5, files.size());
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/META/meta.json"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db"));

        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db"));
        Assert.assertTrue(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db"));
        Assert.assertFalse(files.contains("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db"));

    }

    public static List<S3ObjectSummary> getObjectSummary()
    {
        List<S3ObjectSummary> list = new ArrayList<S3ObjectSummary>();
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/META/meta.json");
        list.add(summary);
        return list;
    }

    public static List<S3ObjectSummary> getObjectSummaryEmpty()
    {
        List<S3ObjectSummary> list = new ArrayList<S3ObjectSummary>();
        return list;
    }

    public static List<S3ObjectSummary> getNextObjectSummary()
    {
        List<S3ObjectSummary> list = new ArrayList<S3ObjectSummary>();
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey("test_backup/"+FakeConfiguration.FAKE_REGION+"/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db");
        list.add(summary);
        return list;
    }

}
