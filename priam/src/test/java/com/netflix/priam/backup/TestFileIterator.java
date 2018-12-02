/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.backup;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for backup file iterator
 *
 * @author Praveen Sadhu
 */
public class TestFileIterator {
    private static Injector injector;
    private static Date startTime, endTime;
    private static Calendar cal;

    private static AmazonS3Client s3client;
    private static S3FileSystem s3FileSystem;
    private static IConfiguration conf;
    private static InstanceIdentity factory;
    private static String region;
    private static String bucket = "TESTBUCKET";

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        s3client = new MockAmazonS3Client().getMockInstance();
        new MockObjectListing();

        injector = Guice.createInjector(new BRTestModule());
        conf = injector.getInstance(IConfiguration.class);
        factory = injector.getInstance(InstanceIdentity.class);
        region = factory.getInstanceInfo().getRegion();
        s3FileSystem = injector.getInstance(S3FileSystem.class);
        s3FileSystem.setS3Client(s3client);

        cal = Calendar.getInstance();
        cal.set(2011, 7, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        endTime = cal.getTime();
    }

    // MockAmazonS3Client class
    @Ignore
    static class MockAmazonS3Client extends MockUp<AmazonS3Client> {
        static String bucketName = "";
        static String prefix = "";

        @Mock
        public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
                throws AmazonClientException {
            ObjectListing listing = new ObjectListing();
            listing.setBucketName(listObjectsRequest.getBucketName());
            listing.setPrefix(listObjectsRequest.getPrefix());
            return listing;
        }

        @Mock
        public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
                throws AmazonClientException {
            ObjectListing listing = new ObjectListing();
            listing.setBucketName(previousObjectListing.getBucketName());
            listing.setPrefix(previousObjectListing.getPrefix());
            return new ObjectListing();
        }
    }

    // MockObjectListing class
    @Ignore
    static class MockObjectListing extends MockUp<ObjectListing> {
        static boolean truncated = true;
        static boolean firstcall = true;
        static boolean simfilter = false; // Simulate filtering

        @Mock
        public List<S3ObjectSummary> getObjectSummaries() {
            if (firstcall) {
                firstcall = false;
                if (simfilter) return getObjectSummaryEmpty();
                return getObjectSummary();
            } else {
                if (simfilter) {
                    simfilter = false; // reset
                    return getObjectSummaryEmpty();
                } else truncated = false;
                return getNextObjectSummary();
            }
        }

        @Mock
        public boolean isTruncated() {
            return truncated;
        }
    }

    private Path getClusterPrefix() {
        return Paths.get(
                conf.getBackupLocation(),
                factory.getInstanceInfo().getRegion(),
                conf.getAppName(),
                factory.getInstance().getToken());
    }

    @Test
    public void testIteratorEmptySet() {
        cal.set(2011, 7, 11, 6, 1, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date stime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        Date etime = cal.getTime();
        MockAmazonS3Client.bucketName = bucket;
        MockAmazonS3Client.prefix = getClusterPrefix() + "/20110811";

        Iterator<AbstractBackupPath> fileIterator = s3FileSystem.list(bucket, stime, etime);
        Set<String> files = new HashSet<>();
        while (fileIterator.hasNext()) files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(0, files.size());
    }

    @Test
    public void testIterator() {
        MockObjectListing.truncated = false;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = false;
        MockAmazonS3Client.bucketName = "TESTBUCKET";
        MockAmazonS3Client.prefix = getClusterPrefix() + "/20110811";

        Iterator<AbstractBackupPath> fileIterator = s3FileSystem.list(bucket, startTime, endTime);

        Set<String> files = new HashSet<>();
        while (fileIterator.hasNext()) files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(3, files.size());
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/META/meta.json"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db"));
    }

    @Test
    public void testIteratorTruncated() {
        MockObjectListing.truncated = true;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = false;
        MockAmazonS3Client.bucketName = "TESTBUCKET";
        MockAmazonS3Client.prefix = getClusterPrefix() + "/20110811";

        Iterator<AbstractBackupPath> fileIterator = s3FileSystem.list(bucket, startTime, endTime);

        Set<String> files = new HashSet<>();
        while (fileIterator.hasNext()) files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(5, files.size());
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/META/meta.json"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db"));

        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db"));
    }

    @Test
    public void testIteratorTruncatedOOR() {
        MockObjectListing.truncated = true;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = true;
        MockAmazonS3Client.bucketName = bucket;
        MockAmazonS3Client.prefix = getClusterPrefix() + "/20110811";

        Iterator<AbstractBackupPath> fileIterator = s3FileSystem.list(bucket, startTime, endTime);

        Set<String> files = new HashSet<>();
        while (fileIterator.hasNext()) files.add(fileIterator.next().getRemotePath());
        Assert.assertEquals(2, files.size());
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201107110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201107110430/SST/ks1/cf1/f2.db"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201107110030/META/meta.json"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201107110600/SST/ks1/cf1/f3.db"));

        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db"));
    }

    @Test
    public void testRestorePathIteration() {
        MockObjectListing.truncated = true;
        MockObjectListing.firstcall = true;
        MockObjectListing.simfilter = false;
        MockAmazonS3Client.bucketName = "RESTOREBUCKET";
        MockAmazonS3Client.prefix =
                "test_restore_backup/fake-restore-region/fakerestorecluster"
                        + "/"
                        + factory.getInstance().getToken();
        MockAmazonS3Client.prefix += "/20110811";

        Iterator<AbstractBackupPath> fileIterator =
                s3FileSystem.list(
                        "RESTOREBUCKET/test_restore_backup/fake-restore-region/fakerestorecluster",
                        startTime,
                        endTime);

        Set<String> files = new HashSet<>();
        while (fileIterator.hasNext()) files.add(fileIterator.next().getRemotePath());
        while (fileIterator.hasNext()) files.add(fileIterator.next().getRemotePath());

        Assert.assertEquals(5, files.size());
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/META/meta.json"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db"));

        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db"));
        Assert.assertTrue(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db"));
        Assert.assertFalse(
                files.contains(
                        "test_backup/"
                                + region
                                + "/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db"));
    }

    private static List<S3ObjectSummary> getObjectSummary() {
        List<S3ObjectSummary> list = new ArrayList<>();
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey(
                "test_backup/" + region + "/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey(
                "test_backup/" + region + "/fakecluster/123456/201108110430/SST/ks1/cf1/f2.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey(
                "test_backup/" + region + "/fakecluster/123456/201108110600/SST/ks1/cf1/f3.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey("test_backup/" + region + "/fakecluster/123456/201108110030/META/meta.json");
        list.add(summary);
        return list;
    }

    private static List<S3ObjectSummary> getObjectSummaryEmpty() {
        return new ArrayList<>();
    }

    private static List<S3ObjectSummary> getNextObjectSummary() {
        List<S3ObjectSummary> list = new ArrayList<>();
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey(
                "test_backup/" + region + "/fakecluster/123456/201108110030/SNAP/ks2/cf1/f1.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey(
                "test_backup/" + region + "/fakecluster/123456/201108110430/SST/ks2/cf1/f2.db");
        list.add(summary);
        summary = new S3ObjectSummary();
        summary.setKey(
                "test_backup/" + region + "/fakecluster/123456/201108110600/SST/ks2/cf1/f3.db");
        list.add(summary);
        return list;
    }
}
