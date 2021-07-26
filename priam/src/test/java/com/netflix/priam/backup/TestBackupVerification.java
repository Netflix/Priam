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
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backupv2.IMetaProxy;
import com.netflix.priam.backupv2.MetaV1Proxy;
import com.netflix.priam.backupv2.MetaV2Proxy;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by aagrawal on 1/23/19. */
public class TestBackupVerification {

    private final BackupVerification backupVerification;
    private final IConfiguration configuration;
    private final IBackupStatusMgr backupStatusMgr;
    private final String backupDate = "201812011000";
    private final String backupDateEnd = "201812021000";
    private final Path location =
            Paths.get(
                    "some_bucket/casstestbackup/1049_fake-app/1808575600",
                    BackupFileType.META_V2.toString(),
                    "1859817645000",
                    "SNAPPY",
                    "PLAINTEXT",
                    "meta_v2_201812011000.json");
    private final int numFakeBackups = 10;

    public TestBackupVerification() {
        Injector injector = Guice.createInjector(new BRTestModule());

        backupVerification = injector.getInstance(BackupVerification.class);
        configuration = injector.getInstance(IConfiguration.class);
        backupStatusMgr = injector.getInstance(IBackupStatusMgr.class);
    }

    static class MockMetaV1Proxy extends MockUp<MetaV1Proxy> {
        @Mock
        public BackupVerificationResult isMetaFileValid(AbstractBackupPath metaBackupPath) {
            return getBackupVerificationResult();
        }
    }

    static class MockMetaV2Proxy extends MockUp<MetaV2Proxy> {
        @Mock
        public BackupVerificationResult isMetaFileValid(AbstractBackupPath metaBackupPath) {
            return getBackupVerificationResult();
        }
    }

    @BeforeEach
    @AfterEach
    public void cleanup() {
        new MockMetaV1Proxy();
        new MockMetaV2Proxy();
        FileUtils.deleteQuietly(new File(configuration.getBackupStatusFileLoc()));
    }

    @Test
    public void illegalDateRange() throws UnsupportedTypeException {
        try {
            backupVerification.verifyBackup(BackupVersion.SNAPSHOT_BACKUP, false, null);
            Assertions.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void illegalDateRangeBackupDateRange() throws UnsupportedTypeException {
        try {
            backupVerification.verifyAllBackups(BackupVersion.SNAPSHOT_BACKUP, null);
            Assertions.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void noBackup() throws Exception {
        Optional<BackupVerificationResult> backupVerificationResultOptinal =
                backupVerification.verifyBackup(
                        BackupVersion.SNAPSHOT_BACKUP,
                        false,
                        new DateRange(Instant.now(), Instant.now()));
        Assertions.assertFalse(backupVerificationResultOptinal.isPresent());

        backupVerificationResultOptinal =
                backupVerification.verifyBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        false,
                        new DateRange(Instant.now(), Instant.now()));
        Assertions.assertFalse(backupVerificationResultOptinal.isPresent());
    }

    @Test
    public void noBackupDateRange() throws Exception {
        List<BackupVerificationResult> backupVerificationResults =
                backupVerification.verifyAllBackups(
                        BackupVersion.SNAPSHOT_BACKUP, new DateRange(Instant.now(), Instant.now()));
        Assertions.assertFalse(backupVerificationResults.size() > 0);

        backupVerificationResults =
                backupVerification.verifyAllBackups(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateRange(Instant.now(), Instant.now()));
        Assertions.assertFalse(backupVerificationResults.size() > 0);
    }

    private void setUp() throws Exception {
        Instant start = DateUtil.parseInstant(backupDate);
        for (int i = 0; i < numFakeBackups - 1; i++) {
            backupStatusMgr.finish(
                    getBackupMetaData(
                            BackupVersion.SNAPSHOT_BACKUP,
                            start.plus(i + 1, ChronoUnit.MINUTES),
                            Status.FINISHED));
        }
        backupStatusMgr.finish(
                getBackupMetaData(BackupVersion.SNAPSHOT_BACKUP, start, Status.FINISHED));
        backupStatusMgr.failed(
                getBackupMetaData(
                        BackupVersion.SNAPSHOT_BACKUP,
                        start.plus(20, ChronoUnit.MINUTES),
                        Status.FAILED));
        for (int i = 0; i < numFakeBackups - 1; i++) {
            backupStatusMgr.finish(
                    getBackupMetaData(
                            BackupVersion.SNAPSHOT_META_SERVICE,
                            start.plus(i + 1, ChronoUnit.MINUTES),
                            Status.FINISHED));
        }
        backupStatusMgr.finish(
                getBackupMetaData(BackupVersion.SNAPSHOT_META_SERVICE, start, Status.FINISHED));
    }

    @Test
    public void verifyBackupVersion1() throws Exception {
        setUp();
        // Verify for backup version 1.0
        Optional<BackupVerificationResult> backupVerificationResultOptinal =
                backupVerification.verifyBackup(
                        BackupVersion.SNAPSHOT_BACKUP,
                        false,
                        new DateRange(backupDate + "," + backupDate));
        Assertions.assertTrue(backupVerificationResultOptinal.isPresent());
        Assertions.assertEquals(
                Instant.EPOCH, backupVerificationResultOptinal.get().snapshotInstant);
        Optional<BackupMetadata> backupMetadata =
                backupStatusMgr
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_BACKUP,
                                new DateRange(backupDate + "," + backupDate))
                        .stream()
                        .findFirst();
        Assertions.assertTrue(backupMetadata.isPresent());
        Assertions.assertNotNull(backupMetadata.get().getLastValidated());

        backupMetadata =
                backupStatusMgr
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_META_SERVICE,
                                new DateRange(backupDate + "," + backupDate))
                        .stream()
                        .findFirst();
        Assertions.assertTrue(backupMetadata.isPresent());
        Assertions.assertNull(backupMetadata.get().getLastValidated());
    }

    @Test
    public void verifyBackupVersion1DateRange() throws Exception {
        setUp();
        // Verify for backup version 1.0
        List<BackupVerificationResult> backupVerificationResults =
                backupVerification.verifyAllBackups(
                        BackupVersion.SNAPSHOT_BACKUP,
                        new DateRange(backupDate + "," + backupDateEnd));
        Assertions.assertTrue(!backupVerificationResults.isEmpty());
        Assertions.assertTrue(backupVerificationResults.size() == numFakeBackups);
        backupVerificationResults
                .stream()
                .forEach(b -> Assertions.assertEquals(Instant.EPOCH, b.snapshotInstant));
        List<BackupMetadata> backupMetadata =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_BACKUP,
                        new DateRange(backupDate + "," + backupDateEnd));
        Assertions.assertTrue(!backupMetadata.isEmpty());
        Assertions.assertTrue(backupMetadata.size() == numFakeBackups);
        backupMetadata.stream().forEach(b -> Assertions.assertNotNull(b.getLastValidated()));

        backupMetadata =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateRange(backupDate + "," + backupDateEnd));
        Assertions.assertTrue(!backupMetadata.isEmpty());
        Assertions.assertTrue(backupMetadata.size() == numFakeBackups);
        backupMetadata.stream().forEach(b -> Assertions.assertNull(b.getLastValidated()));
    }

    @Test
    public void verifyBackupVersion2() throws Exception {
        setUp();
        // Verify for backup version 2.0
        Optional<BackupVerificationResult> backupVerificationResultOptinal =
                backupVerification.verifyBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        false,
                        new DateRange(backupDate + "," + backupDate));
        Assertions.assertTrue(backupVerificationResultOptinal.isPresent());
        Assertions.assertEquals(
                Instant.EPOCH, backupVerificationResultOptinal.get().snapshotInstant);
        Assertions.assertEquals("some_random", backupVerificationResultOptinal.get().remotePath);

        Optional<BackupMetadata> backupMetadata =
                backupStatusMgr
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_META_SERVICE,
                                new DateRange(backupDate + "," + backupDate))
                        .stream()
                        .findFirst();
        Assertions.assertTrue(backupMetadata.isPresent());
        Assertions.assertNotNull(backupMetadata.get().getLastValidated());

        // Retry the verification, it should not try and re-verify
        backupVerificationResultOptinal =
                backupVerification.verifyBackup(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        false,
                        new DateRange(backupDate + "," + backupDate));
        Assertions.assertTrue(backupVerificationResultOptinal.isPresent());
        Assertions.assertEquals(
                DateUtil.parseInstant(backupDate),
                backupVerificationResultOptinal.get().snapshotInstant);
        Assertions.assertNotEquals("some_random", backupVerificationResultOptinal.get().remotePath);
        Assertions.assertEquals(
                location.subpath(1, location.getNameCount()).toString(),
                backupVerificationResultOptinal.get().remotePath);

        backupMetadata =
                backupStatusMgr
                        .getLatestBackupMetadata(
                                BackupVersion.SNAPSHOT_BACKUP,
                                new DateRange(backupDate + "," + backupDate))
                        .stream()
                        .findFirst();
        Assertions.assertTrue(backupMetadata.isPresent());
        Assertions.assertNull(backupMetadata.get().getLastValidated());
    }

    @Test
    public void verifyBackupVersion2DateRange() throws Exception {
        setUp();
        // Verify for backup version 2.0
        List<BackupVerificationResult> backupVerificationResults =
                backupVerification.verifyAllBackups(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateRange(backupDate + "," + backupDateEnd));
        Assertions.assertTrue(!backupVerificationResults.isEmpty());
        Assertions.assertTrue(backupVerificationResults.size() == numFakeBackups);
        backupVerificationResults
                .stream()
                .forEach(b -> Assertions.assertEquals(Instant.EPOCH, b.snapshotInstant));
        List<BackupMetadata> backupMetadata =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_META_SERVICE,
                        new DateRange(backupDate + "," + backupDateEnd));
        Assertions.assertTrue(!backupMetadata.isEmpty());
        Assertions.assertTrue(backupMetadata.size() == numFakeBackups);
        backupMetadata.stream().forEach(b -> Assertions.assertNotNull(b.getLastValidated()));

        backupMetadata =
                backupStatusMgr.getLatestBackupMetadata(
                        BackupVersion.SNAPSHOT_BACKUP,
                        new DateRange(backupDate + "," + backupDateEnd));
        Assertions.assertTrue(!backupMetadata.isEmpty());
        Assertions.assertTrue(backupMetadata.size() == numFakeBackups);
        backupMetadata.stream().forEach(b -> Assertions.assertNull(b.getLastValidated()));
    }

    private BackupMetadata getBackupMetaData(
            BackupVersion backupVersion, Instant startTime, Status status) throws Exception {
        BackupMetadata backupMetadata =
                new BackupMetadata(backupVersion, "123", new Date(startTime.toEpochMilli()));
        backupMetadata.setCompleted(
                new Date(startTime.plus(30, ChronoUnit.MINUTES).toEpochMilli()));
        backupMetadata.setStatus(status);
        backupMetadata.setSnapshotLocation(location.toString());
        return backupMetadata;
    }

    private static BackupVerificationResult getBackupVerificationResult() {
        BackupVerificationResult result = new BackupVerificationResult();
        result.valid = true;
        result.manifestAvailable = true;
        result.remotePath = "some_random";
        result.filesMatched = 123;
        result.snapshotInstant = Instant.EPOCH;
        return result;
    }

    @Test
    public void testGetMetaProxy() {
        IMetaProxy metaProxy = backupVerification.getMetaProxy(BackupVersion.SNAPSHOT_META_SERVICE);
        Assertions.assertTrue(metaProxy != null);
    }
}
