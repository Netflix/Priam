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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.compress.CompressionType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestS3FileSystem {
    private static Injector injector;
    private static final Logger logger = LoggerFactory.getLogger(TestS3FileSystem.class);
    private static final File DIR = new File("target/data/KS1/CF1/backups/201108082320/");
    private static BackupMetrics backupMetrics;
    private static String region;
    private static IConfiguration configuration;

    public TestS3FileSystem() {
        if (injector == null) injector = Guice.createInjector(new BRTestModule());

        if (backupMetrics == null) backupMetrics = injector.getInstance(BackupMetrics.class);

        if (configuration == null) configuration = injector.getInstance(IConfiguration.class);

        InstanceInfo instanceInfo = injector.getInstance(InstanceInfo.class);
        region = instanceInfo.getRegion();
    }

    @BeforeClass
    public static void setUp() {
        new MockAmazonS3Client();
        if (!DIR.exists()) DIR.mkdirs();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.cleanDirectory(DIR);
    }

    @Test
    public void testFileUpload() throws Exception {
        AbstractFileSystem fs = injector.getInstance(NullBackupFileSystem.class);
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(localFile(), BackupFileType.META_V2);
        long noOfFilesUploaded = backupMetrics.getUploadRate().count();
        // temporary hack to allow tests to complete in a timely fashion
        // This will be removed once we stop inheriting from AbstractFileSystem
        fs.uploadAndDeleteInternal(backupfile, Instant.EPOCH, 0 /* retries */);
        Assert.assertEquals(1, backupMetrics.getUploadRate().count() - noOfFilesUploaded);
    }

    @Test
    public void testFileUploadDeleteExists() throws Exception {
        IBackupFileSystem fs = injector.getInstance(NullBackupFileSystem.class);
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(localFile(), BackupFileType.SST_V2);
        fs.uploadAndDelete(backupfile, false /* async */);
        Assert.assertTrue(fs.checkObjectExists(Paths.get(backupfile.getRemotePath())));
        // Lets delete the file now.
        List<Path> deleteFiles = Lists.newArrayList();
        deleteFiles.add(Paths.get(backupfile.getRemotePath()));
        fs.deleteRemoteFiles(deleteFiles);
        Assert.assertFalse(fs.checkObjectExists(Paths.get(backupfile.getRemotePath())));
    }


    @Test
    public void testCleanupAdd() throws Exception {
        MockAmazonS3Client.setRuleAvailable(false);
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getId());
        Assert.assertEquals(configuration.getBackupRetentionDays(), rule.getExpirationInDays());
    }

    @Test
    public void testCleanupIgnore() throws Exception {
        MockAmazonS3Client.setRuleAvailable(true);
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getId());
        Assert.assertEquals(configuration.getBackupRetentionDays(), rule.getExpirationInDays());
    }

    @Test
    public void testCleanupUpdate() throws Exception {
        MockAmazonS3Client.setRuleAvailable(true);
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        String clusterPrefix = "casstestbackup/" + region + "/fake-app/";
        MockAmazonS3Client.updateRule(
                MockAmazonS3Client.getBucketLifecycleConfig(clusterPrefix, 2));
        fs.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getId());
        Assert.assertEquals(configuration.getBackupRetentionDays(), rule.getExpirationInDays());
    }

    @Test
    public void testDeleteObjects() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        List<Path> filesToDelete = new ArrayList<>();
        // Empty files
        fs.deleteRemoteFiles(filesToDelete);

        // Lets add some random files now.
        filesToDelete.add(Paths.get("a.txt"));
        fs.deleteRemoteFiles(filesToDelete);

        // Emulate error now.
        try {
            MockAmazonS3Client.emulateError = true;
            fs.deleteRemoteFiles(filesToDelete);
            Assert.assertTrue(false);
        } catch (BackupRestoreException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testGetFileByteBufferNoCompression() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        File testFile = localFile();
        backupPath.parseLocal(testFile, BackupFileType.SST_V2);
        backupPath.setCompression(CompressionType.NONE);
        
        ByteBuffer result = fs.getFileByteBuffer(backupPath);
        
        Assert.assertNotNull(result);
        Assert.assertEquals(testFile.length(), result.remaining());
        
        byte[] expected = new byte[5 << 10];
        Arrays.fill(expected, (byte) 8);
        byte[] actual = new byte[result.remaining()];
        result.get(actual);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testGetFileByteBufferWithSnappyCompression() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        File testFile = localFile();
        backupPath.parseLocal(testFile, BackupFileType.SST_V2);
        backupPath.setCompression(CompressionType.SNAPPY);
        
        ByteBuffer result = fs.getFileByteBuffer(backupPath);
        
        Assert.assertNotNull(result);
        Assert.assertTrue(result.remaining() > 0);
        Assert.assertTrue(result.remaining() < testFile.length()); // Should be compressed
    }

    @Test
    public void testGetFileByteBufferEmptyFile() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        
        String caller = Thread.currentThread().getStackTrace()[1].getMethodName();
        File emptyFile = new File(DIR + caller + "empty-file.db");
        emptyFile.createNewFile();
        
        backupPath.parseLocal(emptyFile, BackupFileType.SST_V2);
        backupPath.setCompression(CompressionType.NONE);
        
        ByteBuffer result = fs.getFileByteBuffer(backupPath);
        
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.remaining());
    }

    @Test(expected = BackupRestoreException.class)
    public void testGetFileByteBufferNonExistentFile() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        
        File nonExistentFile = new File(DIR + "non-existent-file.db");
        backupPath.parseLocal(nonExistentFile, BackupFileType.SST_V2);
        
        fs.getFileByteBuffer(backupPath);
    }

    private File localFile() throws IOException {
        String caller = Thread.currentThread().getStackTrace()[1].getMethodName();
        File file = new File(DIR + caller + "KS1-CF1-ia-1-Data.db");
        if (file.createNewFile()) {
            byte[] data = new byte[5 << 10];
            Arrays.fill(data, (byte) 8);
            try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
                os.write(data);
            }
        }
        Preconditions.checkState(file.exists());
        return file;
    }


    static class MockAmazonS3Client extends MockUp<AmazonS3Client> {
        private boolean ruleAvailable = false;
        static BucketLifecycleConfiguration bconf;
        static boolean emulateError = false;

        @Mock
        public InitiateMultipartUploadResult initiateMultipartUpload(
                InitiateMultipartUploadRequest initiateMultipartUploadRequest)
                throws AmazonClientException {
            return new InitiateMultipartUploadResult();
        }

        public PutObjectResult putObject(PutObjectRequest putObjectRequest)
                throws SdkClientException {
            PutObjectResult result = new PutObjectResult();
            result.setETag("ad");
            return result;
        }

        @Mock
        public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
            return bconf;
        }

        @Mock
        public void setBucketLifecycleConfiguration(
                String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
            bconf = bucketLifecycleConfiguration;
        }

        @Mock
        public DeleteObjectsResult deleteObjects(DeleteObjectsRequest var1)
                throws SdkClientException, AmazonServiceException {
            if (emulateError) throw new AmazonServiceException("Unable to reach AWS");
            return null;
        }

        static BucketLifecycleConfiguration.Rule getBucketLifecycleConfig(
                String prefix, int expirationDays) {
            return new BucketLifecycleConfiguration.Rule()
                    .withExpirationInDays(expirationDays)
                    .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(prefix)))
                    .withStatus(BucketLifecycleConfiguration.ENABLED)
                    .withId(prefix);
        }

        static void setRuleAvailable(boolean ruleAvailable) {
            if (ruleAvailable) {
                bconf = new BucketLifecycleConfiguration();
                if (bconf.getRules() == null) bconf.setRules(Lists.newArrayList());

                List<BucketLifecycleConfiguration.Rule> rules = bconf.getRules();
                String clusterPath = "casstestbackup/" + region + "/fake-app/";

                List<BucketLifecycleConfiguration.Rule> potentialRules =
                        rules.stream()
                                .filter(rule -> rule.getId().equalsIgnoreCase(clusterPath))
                                .collect(Collectors.toList());
                if (potentialRules == null || potentialRules.isEmpty())
                    rules.add(
                            getBucketLifecycleConfig(
                                    clusterPath, configuration.getBackupRetentionDays()));
            }
        }

        static void updateRule(BucketLifecycleConfiguration.Rule updatedRule) {
            List<BucketLifecycleConfiguration.Rule> rules = bconf.getRules();
            Optional<BucketLifecycleConfiguration.Rule> updateRule =
                    rules.stream()
                            .filter(rule -> rule.getId().equalsIgnoreCase(updatedRule.getId()))
                            .findFirst();
            if (updateRule.isPresent()) {
                rules.remove(updateRule.get());
                rules.add(updatedRule);
            } else {
                rules.add(updatedRule);
            }
            bconf.setRules(rules);
        }
    }
}
