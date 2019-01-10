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

package com.netflix.priam.backup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.utils.DateUtil;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by aagrawal on 12/21/18. */
public class TestBackupVerification {

    private static BackupVerification backupVerification;
    private final String backupDate = "201812011000";
    private List<BackupMetadata> backupMetadataList = new ArrayList<>();

    public TestBackupVerification() {
        Injector injector = Guice.createInjector(new BRTestModule());
        backupVerification = injector.getInstance(BackupVerification.class);
    }

    @Before
    public void prepare() throws Exception {
        backupMetadataList.clear();
        Instant start = DateUtil.parseInstant(backupDate);
        backupMetadataList.add(getBackupMetaData(start, Status.FINISHED));
        backupMetadataList.add(getBackupMetaData(start.plus(2, ChronoUnit.HOURS), Status.FAILED));
        backupMetadataList.add(getBackupMetaData(start.plus(4, ChronoUnit.HOURS), Status.FINISHED));
        backupMetadataList.add(getBackupMetaData(start.plus(6, ChronoUnit.HOURS), Status.FAILED));
        backupMetadataList.add(getBackupMetaData(start.plus(8, ChronoUnit.HOURS), Status.FAILED));
    }

    @Test
    public void getLatestBackup() {
        Optional<BackupMetadata> backupMetadata =
                backupVerification.getLatestBackupMetaData(backupMetadataList);
        Instant start = DateUtil.parseInstant(backupDate);
        Assert.assertEquals(
                start.plus(4, ChronoUnit.HOURS), backupMetadata.get().getStart().toInstant());
    }

    @Test
    public void getLatestBackupFailure() throws Exception {
        Optional<BackupMetadata> backupMetadata =
                backupVerification.getLatestBackupMetaData(new ArrayList<>());
        Assert.assertFalse(backupMetadata.isPresent());

        List<BackupMetadata> failList = new ArrayList<>();
        failList.add(getBackupMetaData(DateUtil.getInstant(), Status.FAILED));
        backupMetadata = backupVerification.getLatestBackupMetaData(failList);
        Assert.assertFalse(backupMetadata.isPresent());
    }

    private BackupMetadata getBackupMetaData(Instant startTime, Status status) throws Exception {
        BackupMetadata backupMetadata =
                new BackupMetadata("123", new Date(startTime.toEpochMilli()));
        backupMetadata.setCompleted(
                new Date(startTime.plus(30, ChronoUnit.MINUTES).toEpochMilli()));
        backupMetadata.setStatus(status);
        backupMetadata.setSnapshotLocation("file.txt");
        return backupMetadata;
    }
}
