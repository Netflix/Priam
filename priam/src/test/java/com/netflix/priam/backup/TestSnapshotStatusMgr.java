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
import java.io.File;
import java.util.Date;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 7/11/17. */
public class TestSnapshotStatusMgr {
    private static final Logger logger = LoggerFactory.getLogger(TestSnapshotStatusMgr.class);

    private static IBackupStatusMgr backupStatusMgr;

    @BeforeClass
    public static void setup() {
        Injector injector = Guice.createInjector(new BRTestModule());
        // cleanup old saved file, if any
        IConfiguration configuration = injector.getInstance(IConfiguration.class);
        File f = new File(configuration.getBackupStatusFileLoc());
        if (f.exists()) f.delete();

        backupStatusMgr = injector.getInstance(IBackupStatusMgr.class);
    }

    @Test
    public void testSnapshotStatusAddFinish() throws Exception {
        Date startTime = DateUtil.getDate("198407110720");

        BackupMetadata backupMetadata = new BackupMetadata("123", startTime);
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

        BackupMetadata backupMetadata = new BackupMetadata("123", startTime);
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
            BackupMetadata backupMetadata = new BackupMetadata("123", time);
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
            BackupMetadata backupMetadata = new BackupMetadata("123", time);
            backupStatusMgr.start(backupMetadata);
            backupStatusMgr.finish(backupMetadata);
        }

        // Verify there is only capacity entries
        Assert.assertEquals(
                backupStatusMgr.getCapacity(), backupStatusMgr.getAllSnapshotStatus().size());
    }
}
