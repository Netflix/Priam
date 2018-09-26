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
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.backup.AbstractBackup;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backupv2.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.BackupFileUtils;
import com.netflix.priam.utils.DateUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * Created by aagrawal on 6/20/18.
 */
public class TestSnapshotMetaService {
    private static final Logger logger = LoggerFactory.getLogger(TestSnapshotMetaService.class.getName());
    private static Path dummyDataDirectoryLocation;
    private static IConfiguration configuration;
    private static IBackupRestoreConfig backupRestoreConfig;
    private static SnapshotMetaService snapshotMetaService;
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

        if (metaFileReader == null)
            metaFileReader = new TestMetaFileReader();

        if (prefixGenerator == null)
            prefixGenerator = injector.getInstance(PrefixGenerator.class);

        dummyDataDirectoryLocation = Paths.get(configuration.getDataFileLocation());
        BackupFileUtils.cleanupDir(dummyDataDirectoryLocation);

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
        String fileName = MetaFileInfo.getMetaFileName(DateUtil.getInstant());
        Path path = Paths.get(dummyDataDirectoryLocation.toFile().getAbsolutePath(), fileName);
        Assert.assertTrue(metaFileReader.isValidMetaFile(path));
        path = Paths.get(dummyDataDirectoryLocation.toFile().getAbsolutePath(), fileName + ".tmp");
        Assert.assertFalse(metaFileReader.isValidMetaFile(path));
    }

    private void test(int noOfSstables, int noOfKeyspaces, int noOfCf) throws Exception{
        Instant snapshotInstant = DateUtil.getInstant();
        String snapshotName = snapshotMetaService.generateSnapshotName(snapshotInstant);
        BackupFileUtils.generateDummyFiles(dummyDataDirectoryLocation, noOfKeyspaces, noOfCf, noOfSstables, AbstractBackup.SNAPSHOT_FOLDER, snapshotName);
        snapshotMetaService.setSnapshotName(snapshotName);
        Path metaFileLocation = snapshotMetaService.processSnapshot(snapshotInstant).getMetaFilePath();
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
        BackupFileUtils.cleanupDir(dummyDataDirectoryLocation);
    }

    @Test
    public void testMetaFile() throws Exception {
        test(5, 1,1);
    }

    @Test
    public void testSize() throws Exception {
        test (1000, 2,2);
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
