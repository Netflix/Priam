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
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.restore.Restore;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRestore {
    private static FakeBackupFileSystem filesystem;
    private static ArrayList<String> fileList;
    private static Calendar cal;
    private static FakeConfiguration conf;
    private static String region;
    private static Restore restore;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        Injector injector = Guice.createInjector(new BRTestModule());
        filesystem =
                (FakeBackupFileSystem)
                        injector.getInstance(
                                Key.get(IBackupFileSystem.class, Names.named("backup")));
        conf = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        region = injector.getInstance(InstanceInfo.class).getRegion();
        restore = injector.getInstance(Restore.class);
        fileList = new ArrayList<>();
        cal = Calendar.getInstance();
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
        File tmpdir = new File(conf.getDataFileLocation() + "/test");
        tmpdir.mkdir();
        Assert.assertTrue(tmpdir.exists());
        cal.set(2011, Calendar.AUGUST, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        restore.restore(startTime, cal.getTime());
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
        tmpdir = new File(conf.getDataFileLocation() + "/test");
        Assert.assertFalse(tmpdir.exists());
    }

    // Pick latest file
    @Test
    public void testRestoreLatest() throws Exception {
        populateBackupFileSystem("test_backup");
        String metafile =
                "test_backup/" + region + "/fakecluster/123456/201108110130/META/meta.json";
        filesystem.addFile(metafile);
        cal.set(2011, Calendar.AUGUST, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        restore.restore(startTime, cal.getTime());
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(metafile));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
    }

    @Test
    public void testNoSnapshots() throws Exception {
        try {
            filesystem.setupTest(fileList);
            cal.set(2011, Calendar.SEPTEMBER, 11, 0, 30);
            Date startTime = cal.getTime();
            cal.add(Calendar.HOUR, 5);
            restore.restore(startTime, cal.getTime());
            Assert.assertFalse(true); // No exception thrown
        } catch (IllegalStateException e) {
            // We are ok. No snapshot found.
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testRestoreFromDiffCluster() throws Exception {
        populateBackupFileSystem("test_backup_new");
        cal.set(2011, Calendar.AUGUST, 11, 0, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date startTime = cal.getTime();
        cal.add(Calendar.HOUR, 5);
        restore.restore(startTime, cal.getTime());
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(0)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(1)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(2)));
        Assert.assertTrue(filesystem.downloadedFiles.contains(fileList.get(3)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(4)));
        Assert.assertFalse(filesystem.downloadedFiles.contains(fileList.get(5)));
    }
}
