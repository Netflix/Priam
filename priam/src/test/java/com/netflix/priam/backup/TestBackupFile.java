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
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.DateUtil;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBackupFile {
    private static Injector injector;
    private static String region;

    @BeforeClass
    public static void setup() throws IOException {
        injector = Guice.createInjector(new BRTestModule());
        File file =
                new File("target/data/Keyspace1/Standard1/", "Keyspace1-Standard1-ia-5-Data.db");
        if (!file.exists()) {
            File dir1 = new File("target/data/Keyspace1/Standard1/");
            if (!dir1.exists()) dir1.mkdirs();
            byte b = 8;
            long oneKB = (1024L);
            System.out.println(oneKB);
            BufferedOutputStream bos1 = new BufferedOutputStream(new FileOutputStream(file));
            for (long i = 0; i < oneKB; i++) {
                bos1.write(b);
            }
            bos1.flush();
            bos1.close();
        }
        InstanceIdentity factory = injector.getInstance(InstanceIdentity.class);
        factory.getInstance().setToken("1234567"); // Token
        region = factory.getInstanceInfo().getRegion();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        File file = new File("Keyspace1-Standard1-ia-5-Data.db");
        FileUtils.deleteQuietly(file);
    }

    @Test
    public void testBackupFileCreation() throws ParseException {
        // Test snapshot file
        String snapshotfile =
                "target/data/Keyspace1/Standard1/snapshots/201108082320/Keyspace1-Standard1-ia-5-Data.db";
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(new File(snapshotfile), BackupFileType.SNAP);
        Assert.assertEquals(BackupFileType.SNAP, backupfile.type);
        Assert.assertEquals("Keyspace1", backupfile.keyspace);
        Assert.assertEquals("Standard1", backupfile.columnFamily);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals(region, backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals(
                "casstestbackup/"
                        + region
                        + "/fake-app/1234567/201108082320/SNAP/Keyspace1/Standard1/Keyspace1-Standard1-ia-5-Data.db",
                backupfile.getRemotePath());
    }

    @Test
    public void testIncBackupFileCreation() throws ParseException {
        // Test incremental file
        File bfile = new File("target/data/Keyspace1/Standard1/Keyspace1-Standard1-ia-5-Data.db");
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(bfile, BackupFileType.SST);
        Assert.assertEquals(BackupFileType.SST, backupfile.type);
        Assert.assertEquals("Keyspace1", backupfile.keyspace);
        Assert.assertEquals("Standard1", backupfile.columnFamily);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals(region, backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        String datestr = DateUtil.formatyyyyMMddHHmm(new Date(bfile.lastModified()));
        Assert.assertEquals(
                "casstestbackup/"
                        + region
                        + "/fake-app/1234567/"
                        + datestr
                        + "/SST/Keyspace1/Standard1/Keyspace1-Standard1-ia-5-Data.db",
                backupfile.getRemotePath());
    }

    @Test
    public void testMetaFileCreation() throws ParseException {
        // Test snapshot file
        String filestr = "cass/data/1234567.meta";
        File bfile = new File(filestr);
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(bfile, BackupFileType.META);
        backupfile.setTime(DateUtil.getDate("201108082320"));
        Assert.assertEquals(BackupFileType.META, backupfile.type);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals(region, backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals(
                "casstestbackup/" + region + "/fake-app/1234567/201108082320/META/1234567.meta",
                backupfile.getRemotePath());
    }
}
