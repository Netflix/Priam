/*
 * Copyright 2019 Netflix, Inc.
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
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.tuner.CassandraTunerService;
import com.netflix.priam.tuner.TuneCassandra;
import com.netflix.priam.utils.BackupFileUtils;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Set;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;

/** Created by aagrawal on 3/10/19. */
public class TestBackupService {
    private final PriamScheduler scheduler;
    private final CassandraTunerService cassandraTunerService;

    public TestBackupService() {
        Injector injector = Guice.createInjector(new BRTestModule());
        this.scheduler = injector.getInstance(PriamScheduler.class);
        this.cassandraTunerService = injector.getInstance(CassandraTunerService.class);
    }

    @Before
    public void cleanup() throws SchedulerException {
        scheduler.getScheduler().clear();
    }

    @Test
    public void testBackupDisabled(
            @Mocked IConfiguration configuration, @Mocked IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "-1";
                configuration.getDataFileLocation();
                result = "target/data";
            }
        };

        Path dummyDataDirectoryLocation = Paths.get(configuration.getDataFileLocation());
        Instant snapshotInstant = DateUtil.getInstant();

        // Create one V1 snapshot.
        String snapshotV1Name = DateUtil.formatInstant(DateUtil.yyyyMMdd, snapshotInstant);
        BackupFileUtils.generateDummyFiles(
                dummyDataDirectoryLocation,
                2,
                3,
                3,
                AbstractBackup.SNAPSHOT_FOLDER,
                snapshotV1Name,
                true);

        String snapshotName = "meta_v2_" + snapshotV1Name;
        // Create one V2 snapshot.
        BackupFileUtils.generateDummyFiles(
                dummyDataDirectoryLocation,
                2,
                3,
                3,
                AbstractBackup.SNAPSHOT_FOLDER,
                snapshotName,
                false);

        IService backupService =
                new BackupService(
                        configuration, backupRestoreConfig, scheduler, cassandraTunerService);
        backupService.scheduleService();
        Assert.assertEquals(0, scheduler.getScheduler().getJobKeys(null).size());

        // snapshot V1 name should not be there.
        Set<Path> backupPaths =
                AbstractBackup.getBackupDirectories(configuration, AbstractBackup.SNAPSHOT_FOLDER);
        for (Path backupPath : backupPaths) {
            Assert.assertTrue(Files.exists(Paths.get(backupPath.toString(), snapshotName)));
            Assert.assertFalse(Files.exists(Paths.get(backupPath.toString(), snapshotV1Name)));
        }
    }

    @Test
    public void testBackupEnabled(
            @Mocked IConfiguration configuration, @Mocked IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "0 0/1 * 1/1 * ? *";
                configuration.isIncrementalBackupEnabled();
                result = false;
            }
        };
        IService backupService =
                new BackupService(
                        configuration, backupRestoreConfig, scheduler, cassandraTunerService);
        backupService.scheduleService();
        Assert.assertEquals(2, scheduler.getScheduler().getJobKeys(null).size());
    }

    @Test
    public void testBackupEnabledWithIncremental(
            @Mocked IConfiguration configuration, @Mocked IBackupRestoreConfig backupRestoreConfig)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "0 0/1 * 1/1 * ? *";
                configuration.isIncrementalBackupEnabled();
                result = true;
            }
        };
        IService backupService =
                new BackupService(
                        configuration, backupRestoreConfig, scheduler, cassandraTunerService);
        backupService.scheduleService();
        Assert.assertEquals(3, scheduler.getScheduler().getJobKeys(null).size());
    }

    @Test
    public void updateService(
            @Mocked IConfiguration configuration,
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Mocked JMXNodeTool nodeTool,
            @Mocked TuneCassandra tuneCassandra)
            throws Exception {
        new Expectations() {
            {
                configuration.getBackupCronExpression();
                result = "0 0/1 * 1/1 * ? *";
                result = "0 0/1 * 1/1 * ? *";
                result = "-1";
                result = "-1";
                configuration.isIncrementalBackupEnabled();
                result = true;
                backupRestoreConfig.enableV2Backups();
                result = true;
                backupRestoreConfig.getSnapshotMetaServiceCronExpression();
                result = "0 0/1 * 1/1 * ? *";
            }
        };
        IService backupService =
                new BackupService(
                        configuration, backupRestoreConfig, scheduler, cassandraTunerService);
        backupService.scheduleService();
        Assert.assertEquals(3, scheduler.getScheduler().getJobKeys(null).size());

        System.out.println("After updated");
        backupService.onChangeUpdateService();
        System.out.println(scheduler.getScheduler().getJobKeys(null));
        Assert.assertEquals(2, scheduler.getScheduler().getJobKeys(null).size());
    }
}
