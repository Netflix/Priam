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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.config.IConfiguration;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import mockit.Mock;
import mockit.MockUp;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test case to test a snapshot backup and incremental backup
 *
 * @author Praveen Sadhu
 */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
public class TestBackup {
    @Inject
    @Named("backup")
    private IBackupFileSystem filesystem;

    @Inject private IConfiguration config;
    private FakeBackupFileSystem fakeBackupFileSystem;

    @Inject private IncrementalBackup backup;
    private static final Logger logger = LoggerFactory.getLogger(TestBackup.class);
    private static final Set<String> expectedFiles = new HashSet<>();

    @Before
    public void setup() throws InterruptedException, IOException {
        new MockNodeProbe();
        fakeBackupFileSystem = (FakeBackupFileSystem) filesystem;
    }

    @After
    public void cleanup() throws IOException {
        File file = new File(config.getDataFileLocation());
        FileUtils.deleteQuietly(file);
        fakeBackupFileSystem.uploadedFiles.clear();
    }

    @Test
    public void testIncrementalBackup() throws Exception {
        generateIncrementalFiles();
        backup.execute();
        Assert.assertEquals(5, fakeBackupFileSystem.uploadedFiles.size());
        for (String filePath : expectedFiles)
            Assert.assertTrue(fakeBackupFileSystem.uploadedFiles.contains(filePath));
    }

    @Test
    public void testClusterSpecificColumnFamiliesSkippedBefore21() throws Exception {
        String[] columnFamilyDirs = {"schema_columns", "local", "peers", "LocationInfo"};
        testClusterSpecificColumnFamiliesSkipped(columnFamilyDirs);
    }

    @Test
    public void testClusterSpecificColumnFamiliesSkippedFrom21() throws Exception {
        String[] columnFamilyDirs = {
            "schema_columns-296e9c049bec30c5828dc17d3df2132a",
            "local-7ad54392bcdd45d684174c047860b347",
            "peers-37c71aca7ac2383ba74672528af04d4f",
            "LocationInfo-9f5c6374d48633299a0a5094bf9ad1e4"
        };
        testClusterSpecificColumnFamiliesSkipped(columnFamilyDirs);
    }

    private void testClusterSpecificColumnFamiliesSkipped(String[] columnFamilyDirs)
            throws Exception {
        File tmp = new File(config.getDataFileLocation());
        if (tmp.exists()) cleanup(tmp);
        // Generate "data"
        generateIncrementalFiles();
        Set<String> systemfiles = new HashSet<>();
        // Generate system files
        for (String columnFamilyDir : columnFamilyDirs) {
            String columnFamily = columnFamilyDir.split("-")[0];
            systemfiles.add(
                    String.format(
                            config.getDataFileLocation()
                                    + "/system/%s/backups/system-%s-ka-1-Data.db",
                            columnFamilyDir,
                            columnFamily));
            systemfiles.add(
                    String.format(
                            config.getDataFileLocation()
                                    + "/system/%s/backups/system-%s-ka-1-Index.db",
                            columnFamilyDir,
                            columnFamily));
        }
        for (String systemFilePath : systemfiles) {
            File file = new File(systemFilePath);
            genTestFile(file);
            // Not cluster specific columns should be backed up
            if (systemFilePath.contains("schema_columns"))
                expectedFiles.add(file.getAbsolutePath());
        }
        backup.execute();
        Assert.assertEquals(8, fakeBackupFileSystem.uploadedFiles.size());
        for (String filePath : expectedFiles)
            Assert.assertTrue(fakeBackupFileSystem.uploadedFiles.contains(filePath));
    }

    private void generateIncrementalFiles() {
        File tmp = new File(config.getDataFileLocation());
        if (tmp.exists()) cleanup(tmp);
        // Setup
        Path backupPath =
                Paths.get(config.getDataFileLocation(), "Keyspace1", "Standard1", "backups");
        Set<String> files = new HashSet<>();
        files.add(Paths.get(backupPath.toString(), "Keyspace1-Standard1-ia-1-Data.db").toString());
        files.add(Paths.get(backupPath.toString(), "Keyspace1-Standard1-ia-1-Index.db").toString());
        files.add(Paths.get(backupPath.toString(), "Keyspace1-Standard1-ia-2-Data.db").toString());
        files.add(Paths.get(backupPath.toString(), "Keyspace1-Standard1-ia-3-Data.db").toString());

        expectedFiles.clear();
        for (String filePath : files) {
            File file = new File(filePath);
            genTestFile(file);
            expectedFiles.add(file.getAbsolutePath());
        }
    }

    private static void genTestFile(File file) {
        try {
            File parent = file.getParentFile();
            if (!parent.exists()) parent.mkdirs();
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
            for (long i = 0; i < (5L * 1024); i++) bos1.write((byte) 8);
            bos1.flush();
            bos1.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private static void cleanup(File dir) {
        FileUtils.deleteQuietly(dir);
    }

    // Mock Nodeprobe class
    @Ignore
    class MockNodeProbe extends MockUp<NodeProbe> {

        @Mock
        public void takeSnapshot(String snapshotName, String columnFamily, String... keyspaces) {
            File tmp = new File(config.getDataFileLocation());
            if (tmp.exists()) cleanup(tmp);
            // Setup
            Set<String> files = new HashSet<>();
            Path snapshotPath =
                    Paths.get(
                            config.getDataFileLocation(),
                            "Keyspace1",
                            "Standard1",
                            "snapshots",
                            snapshotName);
            files.add(snapshotPath.toString() + "/Keyspace1-Standard1-ia-5-Data.db");
            files.add(snapshotPath.toString() + "/Keyspace1-Standard1-ia-6-Data.db");
            files.add(snapshotPath.toString() + "/Keyspace1-Standard1-ia-7-Data.db");

            expectedFiles.clear();
            for (String filePath : files) {
                File file = new File(filePath);
                genTestFile(file);
                if (!filePath.contains("Keyspace1-Standard1-ia-6-Data.db")) // skip
                expectedFiles.add(file.getAbsolutePath());
            }
        }

        @Mock
        public void clearSnapshot(String tag, String... keyspaces) {
            cleanup(new File(config.getDataFileLocation()));
        }
    }
}
