/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.services;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackup;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backupv2.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.scheduler.TaskTimer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by aagrawal on 6/20/18.
 */
public class TestSnapshotMetaService {
    private static final Logger logger = LoggerFactory.getLogger(TestSnapshotMetaService.class.getName());
    private static Path dummyDataDirectoryLocation;
    private static IConfiguration configuration;
    private static IBackupRestoreConfig backupRestoreConfig;
    private static SnapshotMetaService snapshotMetaService;
    private static MetaFileWriter metaFileWriter;
    private static TestMetaFileReader metaFileReader;
    private static PrefixGenerator prefixGenerator;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());

        if (configuration == null)
            configuration = injector.getInstance(IConfiguration.class);

        if (backupRestoreConfig == null)
            backupRestoreConfig = injector.getInstance(IBackupRestoreConfig.class);

        if (snapshotMetaService == null)
            snapshotMetaService = injector.getInstance(SnapshotMetaService.class);

        if (metaFileWriter == null)
            metaFileWriter = injector.getInstance(MetaFileWriter.class);

        if (metaFileReader == null)
            metaFileReader = new TestMetaFileReader();

        if (prefixGenerator == null)
            prefixGenerator = injector.getInstance(PrefixGenerator.class);

        dummyDataDirectoryLocation = Paths.get(configuration.getDataFileLocation());
        cleanupDir(dummyDataDirectoryLocation);

    }

    @Test
    public void testSnapshotMetaServiceEnabled() throws Exception {
        TaskTimer taskTimer = SnapshotMetaService.getTimer(backupRestoreConfig);
        Assert.assertNotNull(taskTimer);
    }

    @Test
    public void testPrefix() throws Exception{
        Assert.assertTrue(prefixGenerator.getPrefix().endsWith("ppa-ekaf/1808575600"));
        Assert.assertTrue(prefixGenerator.getMetaPrefix().endsWith("ppa-ekaf/1808575600/META"));
    }

    @Test
    public void testMetaFileName() throws Exception {
        String fileName = MetaFileInfo.getMetaFileName();
        Path path = Paths.get(dummyDataDirectoryLocation.toFile().getAbsolutePath(), fileName);
        Assert.assertTrue(metaFileReader.isValidMetaFile(path));
        path = Paths.get(dummyDataDirectoryLocation.toFile().getAbsolutePath(), fileName + ".tmp");
        Assert.assertFalse(metaFileReader.isValidMetaFile(path));
    }

    @Test
    public void testMetaFile() throws Exception {
        int noOfSstables = 5;
        String snapshotName = snapshotMetaService.generateSnapshotName();
        generateDummyFiles(dummyDataDirectoryLocation, 1, 1, noOfSstables, AbstractBackup.SNAPSHOT_FOLDER, snapshotName);
        snapshotMetaService.setSnapshotName(snapshotName);
        Path metaFileLocation = snapshotMetaService.processSnapshot();
        Assert.assertNotNull(metaFileLocation);
        Assert.assertTrue(metaFileLocation.toFile().exists());
        Assert.assertTrue(metaFileLocation.toFile().isFile());

        //Try reading meta file.
        metaFileReader.setNoOfSstables(noOfSstables + 1);
        metaFileReader.readMeta(metaFileLocation);

        MetaFileInfo metaFileInfo = metaFileReader.getMetaFileInfo();
        Assert.assertEquals(1, metaFileInfo.getVersion());
        Assert.assertEquals(configuration.getAppName(), metaFileInfo.getAppName());
        Assert.assertEquals(configuration.getRac(), metaFileInfo.getRack());
        Assert.assertEquals(configuration.getDC(), metaFileInfo.getRegion());

        //Cleanup
        metaFileLocation.toFile().delete();
        cleanupDir(dummyDataDirectoryLocation);
    }

    private void cleanupDir(Path dir){
        if (dir.toFile().exists())
            try {
                FileUtils.cleanDirectory(dir.toFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Test
    public void testSize() throws Exception {
        String snapshotName = snapshotMetaService.generateSnapshotName();
        generateDummyFiles(dummyDataDirectoryLocation, 1, 1, 5000, AbstractBackup.SNAPSHOT_FOLDER, snapshotName);
        snapshotMetaService.setSnapshotName(snapshotName);
        Path metaFileLocation = snapshotMetaService.processSnapshot();

        //Validate meta file exists.
        Assert.assertNotNull(metaFileLocation);
        Assert.assertTrue(metaFileLocation.toFile().exists());
        Assert.assertTrue(metaFileLocation.toFile().isFile());
        //Cleanup
        metaFileLocation.toFile().delete();
        cleanupDir(dummyDataDirectoryLocation);
    }

    private void generateDummyFiles(Path dummyDir, int noOfKeyspaces, int noOfCf, int noOfSstables, String backupDir, String snapshotName) throws Exception {
        if (dummyDir == null)
            dummyDir = dummyDataDirectoryLocation;

        //Clean the dummy directory
        if (dummyDir.toFile().exists())
            FileUtils.cleanDirectory(dummyDir.toFile());

        Random random = new Random();

        for (int i = 1; i <= noOfKeyspaces; i++) {
            String keyspaceName = "sample" + i;

            for (int j = 1; j <= noOfCf; j++) {
                String columnfamilyname = "cf" + j;

                for (int k = 1; k <= noOfSstables; k++) {
                    String prefixName = "mc-" + k + "-big";

                    for (Component.Type type : EnumSet.allOf(Component.Type.class)) {
                        Path componentPath = Paths.get(dummyDir.toFile().getAbsolutePath(), keyspaceName, columnfamilyname, backupDir, snapshotName, prefixName + "-" + type.name() + ".db");
                        componentPath.getParent().toFile().mkdirs();
                        try (FileWriter fileWriter = new FileWriter(componentPath.toFile())) {
                            fileWriter.write("");
                        }

                    }
                }

                Path componentPath = Paths.get(dummyDir.toFile().getAbsolutePath(), keyspaceName, columnfamilyname, backupDir, snapshotName, "manifest.json");
                try(FileWriter fileWriter = new FileWriter(componentPath.toFile())){
                    fileWriter.write("");
                }
            }
        }
    }

    public static class TestMetaFileReader extends MetaFileReader {

        private int noOfSstables;

        public void setNoOfSstables(int noOfSstables) {
            this.noOfSstables = noOfSstables;
        }

        @Override
        public void process(ColumnfamilyResult columnfamilyResult) {
            Assert.assertEquals(noOfSstables, columnfamilyResult.getSstables().size());
        }
    }


}
