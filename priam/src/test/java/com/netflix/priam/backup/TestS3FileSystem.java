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

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
        new MockS3PartUploader();
        new MockAmazonS3Client();
        if (!DIR.exists()) DIR.mkdirs();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.cleanDirectory(DIR);
    }

    @Test
    public void testFileUpload() throws Exception {
        MockS3PartUploader.setup();
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
        MockS3PartUploader.setup();
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
    public void testFileUploadFailures() throws Exception {
        MockS3PartUploader.setup();
        MockS3PartUploader.partFailure = true;
        long noOfFailures = backupMetrics.getInvalidUploads().count();
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(localFile(), BackupFileType.META_V2);
        try {
            // temporary hack to allow tests to complete in a timely fashion
            // This will be removed once we stop inheriting from AbstractFileSystem
            fs.uploadAndDeleteInternal(backupfile, Instant.EPOCH, 0 /* retries */);
        } catch (BackupRestoreException e) {
            // ignore
        }
        Assert.assertEquals(0, MockS3PartUploader.compattempts);
        Assert.assertEquals(1, backupMetrics.getInvalidUploads().count() - noOfFailures);
    }

    @Test
    public void testFileUploadCompleteFailure() throws Exception {
        MockS3PartUploader.setup();
        MockS3PartUploader.completionFailure = true;
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        fs.setS3Client(new MockAmazonS3Client().getMockInstance());
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(localFile(), BackupFileType.META_V2);
        try {
            // temporary hack to allow tests to complete in a timely fashion
            // This will be removed once we stop inheriting from AbstractFileSystem
            fs.uploadAndDeleteInternal(backupfile, Instant.EPOCH, 0 /* retries */);
        } catch (BackupRestoreException e) {
            // ignore
        }
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

    // Mock Nodeprobe class
    static class MockS3PartUploader extends MockUp<S3PartUploader> {
        static int compattempts = 0;
        static int partAttempts = 0;
        static boolean partFailure = false;
        static boolean completionFailure = false;
        private static List<CompletedPart> partETags;

        @Mock
        public void $init(S3Client client, DataPart dp, List<CompletedPart> partETags) {
            MockS3PartUploader.partETags = partETags;
        }

        @Mock
        private Void uploadPart() throws SdkException, BackupRestoreException {
            ++partAttempts;
            if (partFailure) throw new BackupRestoreException("Test exception");
            partETags.add(CompletedPart.builder().partNumber(0).eTag(null).build());
            return null;
        }

        @Mock
        public CompleteMultipartUploadResponse completeUpload() throws BackupRestoreException {
            ++compattempts;
            if (completionFailure) throw new BackupRestoreException("Test exception");

            return null;
        }

        @Mock
        public Void retriableCall() throws SdkException, BackupRestoreException {
            logger.info("MOCK UPLOADING...");
            return uploadPart();
        }

        public static void setup() {
            compattempts = 0;
            partAttempts = 0;
            partFailure = false;
            completionFailure = false;
        }
    }

    static class MockAmazonS3Client extends MockUp<S3Client> {
        static boolean emulateError = false;

        @Mock
        public CreateMultipartUploadResponse createMultipartUpload(
                CreateMultipartUploadRequest request) throws SdkException {
            return CreateMultipartUploadResponse.builder()
                    .uploadId("test-upload-id")
                    .build();
        }

        @Mock
        public PutObjectResponse putObject(PutObjectRequest request, software.amazon.awssdk.core.sync.RequestBody body)
                throws SdkException {
            return PutObjectResponse.builder()
                    .eTag("ad")
                    .build();
        }

        @Mock
        public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request)
                throws SdkException {
            if (emulateError) throw S3Exception.builder()
                    .message("Unable to reach AWS")
                    .statusCode(500)
                    .build();
            return DeleteObjectsResponse.builder().build();
        }
    }
}
