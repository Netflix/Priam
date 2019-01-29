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
package com.netflix.priam.utils;

import com.netflix.priam.backup.BackupMetadata;
import com.netflix.priam.backup.BackupVersion;
import com.netflix.priam.health.InstanceState;
import java.time.LocalDateTime;
import java.util.Calendar;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 10/12/17. */
public class TestGsonJsonSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(TestGsonJsonSerializer.class);

    @Test
    public void testBackupMetaData() throws Exception {
        BackupMetadata metadata =
                new BackupMetadata(
                        BackupVersion.SNAPSHOT_BACKUP, "123", Calendar.getInstance().getTime());
        String json = metadata.toString();
        LOG.info(json);
        // Deserialize it.
        BackupMetadata metadata1 =
                GsonJsonSerializer.getGson().fromJson(json, BackupMetadata.class);
        LOG.info(metadata1.toString());
        Assert.assertEquals(metadata.getSnapshotDate(), metadata1.getSnapshotDate());
        Assert.assertEquals(metadata.getToken(), metadata1.getToken());
    }

    @Test
    public void testRestoreStatus() throws Exception {
        InstanceState.RestoreStatus restoreStatus = new InstanceState.RestoreStatus();
        restoreStatus.setStartDateRange(LocalDateTime.now().minusDays(2).withSecond(0).withNano(0));
        restoreStatus.setEndDateRange(LocalDateTime.now().minusHours(3).withSecond(0).withNano(0));
        restoreStatus.setExecutionStartTime(LocalDateTime.now().withSecond(0).withNano(0));
        LOG.info(restoreStatus.toString());

        InstanceState.RestoreStatus restoreStatus1 =
                GsonJsonSerializer.getGson()
                        .fromJson(restoreStatus.toString(), InstanceState.RestoreStatus.class);
        LOG.info(restoreStatus1.toString());

        Assert.assertEquals(
                restoreStatus.getExecutionStartTime(), restoreStatus1.getExecutionStartTime());
        Assert.assertEquals(restoreStatus.getStartDateRange(), restoreStatus1.getStartDateRange());
        Assert.assertEquals(restoreStatus.getEndDateRange(), restoreStatus1.getEndDateRange());
    }
}
