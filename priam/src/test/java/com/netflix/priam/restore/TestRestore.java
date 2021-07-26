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

package com.netflix.priam.restore;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.FakeBackupFileSystem;
import com.netflix.priam.backup.Status;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.DateUtil;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRestore {
    private static FakeBackupFileSystem filesystem;
    private static ArrayList<String> fileList = new ArrayList<>();
    private static FakeConfiguration conf;
    private static String region;
    private static Restore restore;
    private static InstanceState instanceState;

    @BeforeAll
    public static void setup() throws InterruptedException, IOException {
        Injector injector = Guice.createInjector(new BRTestModule());
        if (filesystem == null) filesystem = injector.getInstance(FakeBackupFileSystem.class);
        if (conf == null) conf = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        region = injector.getInstance(InstanceInfo.class).getRegion();
        if (restore == null) restore = injector.getInstance(Restore.class);
        if (instanceState == null) instanceState = injector.getInstance(InstanceState.class);
    }

    private static void populateBackupFileSystem(String baseDir) {
        fileList.clear();
        fileList.add(baseDir + "/" + region + "/fakecluster/123456/201108110030/META/meta.json");
        fileList.add(
                baseDir + "/" + region + "/fakecluster/123456/201108110030/SNAP/ks1/cf1/f1.db");
        fileList.add(
                baseDir + "/" + region + "/fakecluster/123456/201108110030/SNAP/ks1/cf1/f2.db");
        fileList.add(
                baseDir + "/" + region + "/fakecluster/123456/201108110030/SNAP/ks2/cf1/f2.db");
        fileList.add(baseDir + "/" + region + "/fakecluster/123456/201108110530/SST/ks2/cf1/f3.db");
        fileList.add(baseDir + "/" + region + "/fakecluster/123456/201108110600/SST/ks2/cf1/f4.db");
        filesystem.setupTest(fileList);
        conf.setRestorePrefix("RESTOREBUCKET/" + baseDir + "/" + region + "/fakecluster");
    }

    @Test
    public void testRestore() throws Exception {
        populateBackupFileSystem("test_backup");
        String dateRange = "201108110030,201108110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assertions.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreWithIncremental() throws Exception {
        populateBackupFileSystem("test_backup");
        String dateRange = "201108110030,201108110730";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assertions.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreLatestWithEmptyMeta() throws Exception {
        populateBackupFileSystem("test_backup");
        String metafile =
                "test_backup/" + region + "/fakecluster/123456/201108110130/META/meta.json";
        filesystem.addFile(metafile);
        String dateRange = "201108110030,201108110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(metafile));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assertions.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
        Assertions.assertEquals(metafile, instanceState.getRestoreStatus().getSnapshotMetaFile());
    }

    @Test
    public void testRestoreLatest() throws Exception {
        populateBackupFileSystem("test_backup");
        String metafile =
                "test_backup/" + region + "/fakecluster/123456/201108110130/META/meta.json";
        filesystem.addFile(metafile);
        String snapFile =
                "test_backup/" + region + "/fakecluster/123456/201108110130/SNAP/ks1/cf1/f9.db";
        filesystem.addFile(snapFile);
        String dateRange = "201108110030,201108110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(metafile));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(snapFile));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assertions.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
        Assertions.assertEquals(metafile, instanceState.getRestoreStatus().getSnapshotMetaFile());
    }

    @Test
    public void testNoSnapshots() throws Exception {
        populateBackupFileSystem("test_backup");
        filesystem.setupTest(fileList);
        String dateRange = "201109110030,201109110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assertions.assertEquals(Status.FAILED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreFromDiffCluster() throws Exception {
        populateBackupFileSystem("test_backup_new");
        String dateRange = "201108110030,201108110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        System.out.println("Downloaded files: " + filesystem.downloadedFiles);
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assertions.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assertions.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assertions.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }
}
