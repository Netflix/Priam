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
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 7/11/17. */
public class TestBackupStatusMgr {
    private static final Logger logger = LoggerFactory.getLogger(TestBackupStatusMgr.class);
    private static IConfiguration configuration;
    private static IBackupStatusMgr backupStatusMgr;
    private final String backupDate = "201812011000";

    @BeforeClass
    public static void setup() {
        Injector injector = Guice.createInjector(new BRTestModule());
        // cleanup old saved file, if any
        configuration = injector.getInstance(IConfiguration.class);
        backupStatusMgr = injector.getInstance(IBackupStatusMgr.class);
    }

    @Before
    @After
    public void cleanup() {
        FileUtils.deleteQuietly(new File(configuration.getBackupStatusFileLoc()));
    }

    private void prepare() throws Exception {
        cleanup();
        Instant start = DateUtil.parseInstant(backupDate);
        backupStatusMgr.finish(getBackupMetaData(start, Status.FINISHED));
        backupStatusMgr.failed(getBackupMetaData(start.plus(2, ChronoUnit.HOURS), Status.FAILED));
        backupStatusMgr.finish(getBackupMetaData(start.plus(4, ChronoUnit.HOURS), Status.FINISHED));
        backupStatusMgr.failed(getBackupMetaData(start.plus(6, ChronoUnit.HOURS), Status.FAILED));
        backupStatusMgr.failed(getBackupMetaData(start.plus(8, ChronoUnit.HOURS), Status.FAILED));
        backupStatusMgr.finish(getBackupMetaData(start.plus(1, ChronoUnit.DAYS), Status.FINISHED));
        backupStatusMgr.finish(getBackupMetaData(start.plus(2, ChronoUnit.DAYS), Status.FINISHED));
    }

    private BackupMetadata getBackupMetaData(Instant startTime, Status status) throws Exception {
        BackupMetadata backupMetadata =
                new BackupMetadata(
                        BackupVersion.SNAPSHOT_BACKUP, "123", new Date(startTime.toEpochMilli()));
        backupMetadata.setCompleted(
                new Date(startTime.plus(30, ChronoUnit.MINUTES).toEpochMilli()));
        backupMetadata.setStatus(status);
        backupMetadata.setSnapshotLocation("file.txt");
        return backupMetadata;
    }

    @Test
    public void testSnapshotUpdateMethod() throws Exception {
        Date startTime = DateUtil.getDate("198407110720");
        BackupMetadata backupMetadata =
                new BackupMetadata(BackupVersion.SNAPSHOT_BACKUP, "123", startTime);
        backupStatusMgr.start(backupMetadata);
        Optional<BackupMetadata> backupMetadata1 =
                backupStatusMgr.locate(startTime).stream().findFirst();
        Assert.assertNull(backupMetadata1.get().getLastValidated());
        backupMetadata.setLastValidated(Calendar.getInstance().getTime());
        backupMetadata.setCassandraSnapshotSuccess(true);
        backupMetadata.setSnapshotLocation("random");
        backupStatusMgr.update(backupMetadata);
        backupMetadata1 = backupStatusMgr.locate(startTime).stream().findFirst();
        Assert.assertNotNull(backupMetadata1.get().getLastValidated());
        Assert.assertTrue(backupMetadata1.get().isCassandraSnapshotSuccess());
        Assert.assertEquals("random", backupMetadata1.get().getSnapshotLocation());
    }

    @Test
    public void testSnapshotStatusAddFinish() throws Exception {
        Date startTime = DateUtil.getDate("198407110720");

        BackupMetadata backupMetadata =
                new BackupMetadata(BackupVersion.SNAPSHOT_BACKUP, "123", startTime);
        backupStatusMgr.start(backupMetadata);
        List<BackupMetadata> metadataList = backupStatusMgr.locate(startTime);
        Assert.assertNotNull(metadataList);
        Assert.assertTrue(!metadataList.isEmpty());
        Assert.assertEquals(1, metadataList.size());
        Assert.assertEquals(startTime, metadataList.get(0).getStart());
        logger.info("Snapshot start: {}", metadataList.get(0));

        backupStatusMgr.finish(backupMetadata);
        metadataList = backupStatusMgr.locate(startTime);
        Assert.assertNotNull(metadataList);
        Assert.assertTrue(!metadataList.isEmpty());
        Assert.assertEquals(1, metadataList.size());
        Assert.assertEquals(Status.FINISHED, metadataList.get(0).getStatus());
        Assert.assertTrue(metadataList.get(0).getCompleted() != null);
        logger.info("Snapshot finished: {}", metadataList.get(0));
    }

    @Test
    public void testSnapshotStatusAddFailed() throws Exception {
        Date startTime = DateUtil.getDate("198407120720");

        BackupMetadata backupMetadata =
                new BackupMetadata(BackupVersion.SNAPSHOT_BACKUP, "123", startTime);
        backupStatusMgr.start(backupMetadata);
        List<BackupMetadata> metadataList = backupStatusMgr.locate(startTime);
        Assert.assertNotNull(metadataList);
        Assert.assertTrue(!metadataList.isEmpty());
        Assert.assertEquals(1, metadataList.size());
        Assert.assertEquals(startTime, metadataList.get(0).getStart());
        logger.info("Snapshot start: {}", metadataList.get(0));

        backupStatusMgr.failed(backupMetadata);
        metadataList = backupStatusMgr.locate(startTime);
        Assert.assertNotNull(metadataList);
        Assert.assertTrue(!metadataList.isEmpty());
        Assert.assertEquals(1, metadataList.size());
        Assert.assertEquals(Status.FAILED, metadataList.get(0).getStatus());
        Assert.assertTrue(metadataList.get(0).getCompleted() != null);
        logger.info("Snapshot failed: {}", metadataList.get(0));
    }

    @Test
    public void testSnapshotStatusMultiAddFinishInADay() throws Exception {
        final int noOfEntries = 10;
        Date startTime = DateUtil.getDate("19840101");

        for (int i = 0; i < noOfEntries; i++) {
            assert startTime != null;
            Date time = new DateTime(startTime.getTime()).plusHours(i).toDate();
            BackupMetadata backupMetadata =
                    new BackupMetadata(BackupVersion.SNAPSHOT_BACKUP, "123", time);
            backupStatusMgr.start(backupMetadata);
            backupStatusMgr.finish(backupMetadata);
        }

        List<BackupMetadata> metadataList = backupStatusMgr.locate(startTime);
        Assert.assertEquals(noOfEntries, metadataList.size());
        logger.info(metadataList.toString());

        // Ensure that list is always maintained from latest to eldest
        Date latest = null;
        for (BackupMetadata backupMetadata : metadataList) {
            if (latest == null) latest = backupMetadata.getStart();
            else {
                Assert.assertTrue(backupMetadata.getStart().before(latest));
                latest = backupMetadata.getStart();
            }
        }
    }

    @Test
    public void testSnapshotStatusSize() throws Exception {
        final int noOfEntries = backupStatusMgr.getCapacity() + 1;
        Date startTime = DateUtil.getDate("19850101");

        for (int i = 0; i < noOfEntries; i++) {
            assert startTime != null;
            Date time = new DateTime(startTime.getTime()).plusDays(i).toDate();
            BackupMetadata backupMetadata =
                    new BackupMetadata(BackupVersion.SNAPSHOT_BACKUP, "123", time);
            backupStatusMgr.start(backupMetadata);
            backupStatusMgr.finish(backupMetadata);
        }

        // Verify there is only capacity entries
        Assert.assertEquals(
                backupStatusMgr.getCapacity(), backupStatusMgr.getAllSnapshotStatus().size());
    }

    @Test
    public void getLatestBackup() throws Exception {
        prepare();
        Instant start = DateUtil.parseInstant(backupDate);
        List<BackupMetadata> list =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_BACKUP,
                        new DateRange(
                                backupDate
                                        + ","
                                        + DateUtil.formatInstant(
                                                DateUtil.yyyyMMddHHmm,
                                                start.plus(12, ChronoUnit.HOURS))));

        Optional<BackupMetadata> backupMetadata = list.stream().findFirst();
        Assert.assertEquals(
                start.plus(4, ChronoUnit.HOURS), backupMetadata.get().getStart().toInstant());
    }

    @Test
    public void getLatestBackupFailure() throws Exception {
        Optional<BackupMetadata> backupMetadata =
                backupStatusMgr
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_BACKUP,
                                new DateRange(backupDate + "," + backupDate))
                        .stream()
                        .findFirst();

        Assert.assertFalse(backupMetadata.isPresent());

        backupStatusMgr.failed(getBackupMetaData(DateUtil.parseInstant(backupDate), Status.FAILED));
        backupMetadata =
                backupStatusMgr
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_BACKUP,
                                new DateRange(backupDate + "," + backupDate))
                        .stream()
                        .findFirst();
        Assert.assertFalse(backupMetadata.isPresent());
    }

    @Test
    public void getLatestBackupMetadata() throws Exception {
        prepare();
        List<BackupMetadata> list =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_BACKUP,
                        new DateRange(backupDate + "," + "201812031000"));
        list.forEach(System.out::println);
    }
}
