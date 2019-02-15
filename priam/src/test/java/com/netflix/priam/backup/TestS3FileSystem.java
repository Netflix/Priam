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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.TestPropertyOverride;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.ReplaceWithMock;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.aws.DataPart;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.S3PartUploader;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.merics.BackupMetrics;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import mockit.Mock;
import mockit.MockUp;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
@TestPropertyOverride({"priam.backup.retention=5"})
public class TestS3FileSystem {
    private static final Logger logger = LoggerFactory.getLogger(TestS3FileSystem.class);
    @Inject private BackupMetrics backupMetrics;
    @Inject private InstanceInfo instanceInfo;
    @Inject private S3FileSystem s3FileSystem;
    @Inject private RemoteBackupPath s3backupPath;
    @Inject private IConfiguration configuration;
    @ReplaceWithMock private AmazonS3Client amazonS3Client;
    @ReplaceWithMock private S3PartUploader s3PartUploader;
    private final String FILE_PATH =
            "target/cass/data/Keyspace1/Standard1/backups/201108082320/Keyspace1-Standard1-ia-1-Data.db";

    private static String region;

    @Before
    public void setup() throws InterruptedException, IOException {
        new MockS3PartUploader();
        new MockAmazonS3Client();

        File file = new File(FILE_PATH);
        file.getParentFile().mkdirs();
        long fiveKB = (5L * 1024);
        byte b = 8;
        BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
        for (long i = 0; i < fiveKB; i++) {
            bos1.write(b);
        }
        bos1.close();
        region = instanceInfo.getRegion();
    }

    @After
    public void cleanup() {
        File file = new File(FILE_PATH);
        file.delete();
    }

    @Test
    public void testS3BackupPathParser() throws Exception {
        s3backupPath.parseLocal(new File(FILE_PATH), BackupFileType.SNAP);
        Assert.assertEquals("Keyspace1", s3backupPath.keyspace);
        Assert.assertEquals("Standard1", s3backupPath.columnFamily);
        Assert.assertNotNull(s3backupPath.getTime());
    }

    @Test
    public void testFilePutUploadSuccess() throws Exception {
        double success = backupMetrics.getValidUploads().actualCount();
        MockAmazonS3Client.putFailure = false;
        s3backupPath.parseLocal(new File(FILE_PATH), BackupFileType.SNAP);
        s3FileSystem.uploadFile(
                Paths.get(s3backupPath.getBackupFile().getAbsolutePath()),
                Paths.get(s3backupPath.getRemotePath()),
                s3backupPath,
                0,
                false);
        Assert.assertEquals(1, (int) (backupMetrics.getValidUploads().actualCount() - success));
    }

    @Test
    public void testFilePutUploadFailure() throws Exception {
        double failure = backupMetrics.getInvalidUploads().actualCount();
        MockAmazonS3Client.putFailure = true;
        s3backupPath.parseLocal(new File(FILE_PATH), BackupFileType.SNAP);
        try {
            s3FileSystem.uploadFile(
                    Paths.get(s3backupPath.getBackupFile().getAbsolutePath()),
                    Paths.get(s3backupPath.getRemotePath()),
                    s3backupPath,
                    0,
                    false);
        } catch (Exception e) {

        }
        Assert.assertEquals(1, (int) (backupMetrics.getInvalidUploads().actualCount() - failure));
    }

    @Test
    @TestPropertyOverride({"priam.backup.chunksizemb=10"})
    public void testFileUploadMultipartFailures() throws Exception {
        MockS3PartUploader.setup();
        MockS3PartUploader.partFailure = true;
        long noOfFailures = backupMetrics.getInvalidUploads().count();
        s3backupPath.parseLocal(new File(FILE_PATH), BackupFileType.SNAP);
        try {
            s3FileSystem.uploadFile(
                    Paths.get(s3backupPath.getBackupFile().getAbsolutePath()),
                    Paths.get(s3backupPath.getRemotePath()),
                    s3backupPath,
                    0,
                    false);
        } catch (BackupRestoreException e) {
            // ignore
        }
        Assert.assertEquals(0, MockS3PartUploader.compattempts);
        Assert.assertEquals(1, backupMetrics.getInvalidUploads().count() - noOfFailures);
    }

    @Test
    @TestPropertyOverride({"priam.backup.chunksizemb=10"})
    public void testFileUploadMultipartCompleteFailure() throws Exception {
        MockS3PartUploader.setup();
        MockS3PartUploader.completionFailure = true;
        s3backupPath.parseLocal(new File(FILE_PATH), BackupFileType.SNAP);
        try {
            s3FileSystem.uploadFile(
                    Paths.get(s3backupPath.getBackupFile().getAbsolutePath()),
                    Paths.get(s3backupPath.getRemotePath()),
                    s3backupPath,
                    0,
                    false);
        } catch (BackupRestoreException e) {
            // ignore
        }
    }

    @Test
    public void testCleanupAdd() throws Exception {
        MockAmazonS3Client.ruleAvailable = false;
        s3FileSystem.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        logger.info(rule.getPrefix());
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getPrefix());
        Assert.assertEquals(5, rule.getExpirationInDays());
    }

    @Test
    public void testCleanupIgnore() throws Exception {
        MockAmazonS3Client.ruleAvailable = true;
        s3FileSystem.cleanup();
        Assert.assertEquals(1, MockAmazonS3Client.bconf.getRules().size());
        BucketLifecycleConfiguration.Rule rule = MockAmazonS3Client.bconf.getRules().get(0);
        logger.info(rule.getPrefix());
        Assert.assertEquals("casstestbackup/" + region + "/fake-app/", rule.getPrefix());
        Assert.assertEquals(5, rule.getExpirationInDays());
    }

    @Test
    public void testDeleteObjects() throws Exception {
        S3FileSystem fs = s3FileSystem;
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

    // Mock Nodeprobe class
    static class MockS3PartUploader extends MockUp<S3PartUploader> {
        static int compattempts = 0;
        static int partAttempts = 0;
        static boolean partFailure = false;
        static boolean completionFailure = false;
        private static List<PartETag> partETags;

        @Mock
        public void $init(AmazonS3 client, DataPart dp, List<PartETag> partETags) {
            MockS3PartUploader.partETags = partETags;
        }

        @Mock
        private Void uploadPart() throws AmazonClientException, BackupRestoreException {
            ++partAttempts;
            if (partFailure) throw new BackupRestoreException("Test exception");
            partETags.add(new PartETag(0, null));
            return null;
        }

        @Mock
        public CompleteMultipartUploadResult completeUpload() throws BackupRestoreException {
            ++compattempts;
            if (completionFailure) throw new BackupRestoreException("Test exception");

            return null;
        }

        @Mock
        public Void retriableCall() throws AmazonClientException, BackupRestoreException {
            logger.info("MOCK UPLOADING...");
            return uploadPart();
        }

        static void setup() {
            compattempts = 0;
            partAttempts = 0;
            partFailure = false;
            completionFailure = false;
        }
    }

    static class MockAmazonS3Client extends MockUp<AmazonS3Client> {
        static boolean ruleAvailable = false;
        static boolean putFailure = false;
        static BucketLifecycleConfiguration bconf = new BucketLifecycleConfiguration();
        static boolean emulateError = false;

        @Mock
        public InitiateMultipartUploadResult initiateMultipartUpload(
                InitiateMultipartUploadRequest initiateMultipartUploadRequest)
                throws AmazonClientException {
            return new InitiateMultipartUploadResult();
        }

        @Mock
        public PutObjectResult putObject(PutObjectRequest putObjectRequest)
                throws SdkClientException, AmazonServiceException {
            if (putFailure) throw new SdkClientException("Put upload failure");

            PutObjectResult result = new PutObjectResult();
            result.setETag("ad");
            return result;
        }

        @Mock
        public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
            List<BucketLifecycleConfiguration.Rule> rules = Lists.newArrayList();
            if (ruleAvailable) {
                String clusterPath = "casstestbackup/" + region + "/fake-app/";
                BucketLifecycleConfiguration.Rule rule =
                        new BucketLifecycleConfiguration.Rule()
                                .withExpirationInDays(5)
                                .withPrefix(clusterPath);
                rule.setStatus(BucketLifecycleConfiguration.ENABLED);
                rule.setId(clusterPath);
                rules.add(rule);
            }
            bconf.setRules(rules);
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
    }
}
