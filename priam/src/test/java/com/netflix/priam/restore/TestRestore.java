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

import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.FakeBackupFileSystem;
import com.netflix.priam.backup.Status;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.utils.DateUtil;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRestore {
    private static FakeBackupFileSystem filesystem;
    private static ArrayList<String> fileList = new ArrayList<>();
    private static FakeConfiguration conf;
    private static Restore restore;
    private static InstanceState instanceState;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        Injector injector = Guice.createInjector(new BRTestModule());
        if (filesystem == null) filesystem = injector.getInstance(FakeBackupFileSystem.class);
        if (conf == null) conf = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        if (restore == null) restore = injector.getInstance(Restore.class);
        if (instanceState == null) instanceState = injector.getInstance(InstanceState.class);
    }

    private static void populateBackupFileSystem(String cluster) {
        fileList.clear();
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/META_V2/1313026200000/SNAPPY/PLAINTEXT/meta_v2_201108110130.json");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/SST_V2/1313022601000/ks1/cf1/SNAPPY/PLAINTEXT/me-1-big-Data.db");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/SST_V2/1313022601000/ks1/cf1/SNAPPY/PLAINTEXT/me-1-big-Index.db");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/SST_V2/1313022601000/ks2/cf1/SNAPPY/PLAINTEXT/me-2-big-Data.db");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/SST_V2/1313040601000/ks2/cf1/SNAPPY/PLAINTEXT/me-2-big-Index.db");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/SST_V2/1313042400000/ks2/cf1/SNAPPY/PLAINTEXT/me-2-big-Summary.db");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/SST_V2/1313043000000/ks1/cf1/SNAPPY/PLAINTEXT/me-3-big-Data.db");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/META_V2/1313043300000/SNAPPY/PLAINTEXT/meta_v2_201108110615.json");
        fileList.add(
                "test_backup/"
                        + cluster
                        + "/1808575600/META_V2/1313022540000/SNAPPY/PLAINTEXT/meta_v2_201108110029.json");
        filesystem.setupTest(fileList);
        conf.setRestorePrefix("RESTOREBUCKET/test_backup/" + cluster + "");
    }

    @Test
    public void testRestore() throws Exception {
        populateBackupFileSystem("1049_fake-app");
        String dateRange = "201108110030,201108110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assert.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreWithIncremental() throws Exception {
        populateBackupFileSystem("1049_fake-app");
        String dateRange = "201108110030,201108110601";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assert.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreWithIncrementalFromDifferentCluster() throws Exception {
        populateBackupFileSystem("6089_fake-app2");
        String dateRange = "201108110030,201108110601";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assert.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreEmptyMeta() throws Exception {
        populateBackupFileSystem("1049_fake-app");
        String metafile =
                "test_backup/1049_fake-app/1808575600/META_V2/1313022540000/SNAPPY/PLAINTEXT/meta_v2_201108110029.json";
        String dateRange = "201108110029,201108110030";
        restore.restore(new DateUtil.DateRange(dateRange));
        Truth.assertThat(filesystem.downloadedFiles).containsExactly(metafile);
        Assert.assertEquals(Status.FAILED, instanceState.getRestoreStatus().getStatus());
        Assert.assertNull(instanceState.getRestoreStatus().getSnapshotMetaFile());
    }

    @Test
    public void testRestoreLatest() throws Exception {
        populateBackupFileSystem("1049_fake-app");
        String metafile =
                "test_backup/1049_fake-app/1808575600/META_V2/1313043300000/SNAPPY/PLAINTEXT/meta_v2_201108110615.json";
        String dateRange = "201108110030,201108110620";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(6)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(7)));
        Assert.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
        Assert.assertEquals(metafile, instanceState.getRestoreStatus().getSnapshotMetaFile());
    }

    @Test
    public void testNoSnapshots() throws Exception {
        populateBackupFileSystem("1049_fake-app");
        filesystem.setupTest(fileList);
        String dateRange = "201109110030,201109110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assert.assertEquals(Status.FAILED, instanceState.getRestoreStatus().getStatus());
    }

    @Test
    public void testRestoreFromDiffCluster() throws Exception {
        populateBackupFileSystem("6089_fake-app2");
        String dateRange = "201108110030,201108110530";
        restore.restore(new DateUtil.DateRange(dateRange));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        Assert.assertEquals(Status.FINISHED, instanceState.getRestoreStatus().getStatus());
    }
}
