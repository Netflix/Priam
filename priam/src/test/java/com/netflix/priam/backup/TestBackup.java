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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import mockit.Mock;
import mockit.MockUp;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test case to test a snapshot backup and incremental backup
 *
 * @author Praveen Sadhu
 */
public class TestBackup {
    private static Injector injector;
    private static FakeBackupFileSystem filesystem;
    private static final Logger logger = LoggerFactory.getLogger(TestBackup.class);
    private static final Set<String> expectedFiles = new HashSet<>();

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        new MockNodeProbe();
        injector = Guice.createInjector(new BRTestModule());
        filesystem =
                (FakeBackupFileSystem)
                        injector.getInstance(
                                Key.get(IBackupFileSystem.class, Names.named("backup")));
    }

    @AfterClass
    public static void cleanup() throws IOException {
        File file = new File("target/data");
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testSnapshotBackup() throws Exception {
        filesystem.cleanup();
        SnapshotBackup backup = injector.getInstance(SnapshotBackup.class);

        //
        //        backup.execute();
        //        Assert.assertEquals(3, filesystem.uploadedFiles.size());
        //        System.out.println("***** "+filesystem.uploadedFiles.size());
        //        boolean metafile = false;
        //        for (String filePath : expectedFiles)
        //            Assert.assertTrue(filesystem.uploadedFiles.contains(filePath));
        //
        //        for(String filepath : filesystem.uploadedFiles){
        //            if( filepath.endsWith("meta.json")){
        //                metafile = true;
        //                break;
        //            }
        //        }
        //        Assert.assertTrue(metafile);

    }

    @Test
    public void testIncrementalBackup() throws Exception {
        filesystem.cleanup();
        generateIncrementalFiles();
        IncrementalBackup backup = injector.getInstance(IncrementalBackup.class);
        backup.execute();
        Assert.assertEquals(4, filesystem.uploadedFiles.size());
        for (String filePath : expectedFiles)
            Assert.assertTrue(filesystem.uploadedFiles.contains(filePath));
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

    @Test
    public void testSkippingEmptyFiles() throws Exception {
        filesystem.cleanup();
        File tmp = new File("target/data/");
        if (tmp.exists()) cleanup(tmp);
        File emptyFile =
                new File(
                        "target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-1-Data.db");
        File parent = emptyFile.getParentFile();
        if (!parent.exists()) parent.mkdirs();
        Assert.assertTrue(emptyFile.createNewFile());
        IncrementalBackup backup = injector.getInstance(IncrementalBackup.class);
        backup.execute();
        Assert.assertTrue(filesystem.uploadedFiles.isEmpty());
        Assert.assertFalse(emptyFile.exists());
    }

    private void testClusterSpecificColumnFamiliesSkipped(String[] columnFamilyDirs)
            throws Exception {
        filesystem.cleanup();
        File tmp = new File("target/data/");
        if (tmp.exists()) cleanup(tmp);
        // Generate "data"
        generateIncrementalFiles();
        Set<String> systemfiles = new HashSet<>();
        // Generate system files
        for (String columnFamilyDir : columnFamilyDirs) {
            String columnFamily = columnFamilyDir.split("-")[0];
            systemfiles.add(
                    String.format(
                            "target/data/system/%s/backups/system-%s-ka-1-Data.db",
                            columnFamilyDir, columnFamily));
            systemfiles.add(
                    String.format(
                            "target/data/system/%s/backups/system-%s-ka-1-Index.db",
                            columnFamilyDir, columnFamily));
        }
        for (String systemFilePath : systemfiles) {
            File file = new File(systemFilePath);
            genTestFile(file);
            // Not cluster specific columns should be backed up
            if (systemFilePath.contains("schema_columns"))
                expectedFiles.add(file.getAbsolutePath());
        }
        IncrementalBackup backup = injector.getInstance(IncrementalBackup.class);
        backup.execute();
        Assert.assertEquals(6, filesystem.uploadedFiles.size());
        for (String filePath : expectedFiles)
            Assert.assertTrue(filesystem.uploadedFiles.contains(filePath));
    }

    private static void generateIncrementalFiles() {
        File tmp = new File("target/data/");
        if (tmp.exists()) cleanup(tmp);
        // Setup
        Set<String> files = new HashSet<>();
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-1-Data.db");
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-1-Index.db");
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-2-Data.db");
        files.add("target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-3-Data.db");

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
    static class MockNodeProbe extends MockUp<NodeProbe> {

        @Mock
        public void takeSnapshot(String snapshotName, String columnFamily, String... keyspaces) {
            File tmp = new File("target/data/");
            if (tmp.exists()) cleanup(tmp);
            // Setup
            Set<String> files = new HashSet<>();
            files.add(
                    "target/data/Keyspace1/Standard1/snapshots/"
                            + snapshotName
                            + "/Keyspace1-Standard1-ia-5-Data.db");
            files.add(
                    "target/data/Keyspace1/Standard1/snapshots/201101081230/Keyspace1-Standard1-ia-6-Data.db");
            files.add(
                    "target/data/Keyspace1/Standard1/snapshots/"
                            + snapshotName
                            + "/Keyspace1-Standard1-ia-7-Data.db");

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
            cleanup(new File("target/data"));
        }
    }
}
