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
import java.io.File;
import java.io.IOException;
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
        InstanceIdentity factory = injector.getInstance(InstanceIdentity.class);
        factory.getInstance().setToken("1234567");
        region = factory.getInstanceInfo().getRegion();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File("target"));
    }

    @Test
    public void testBackupFileCreation() throws IOException {
        File file =
                createFile(
                        "target/data/Keyspace1/Standard1/snapshots/201108082320/Keyspace1-Standard1-ia-5-Data.db");
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(file, BackupFileType.SST_V2);
        Assert.assertEquals(BackupFileType.SST_V2, backupfile.type);
        Assert.assertEquals("Keyspace1", backupfile.keyspace);
        Assert.assertEquals("Standard1", backupfile.columnFamily);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals(region, backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals(
                "casstestbackup/1049_fake-app/1234567/SST_V2/1699206297000/Keyspace1/Standard1/SNAPPY/PLAINTEXT/Keyspace1-Standard1-ia-5-Data.db",
                backupfile.getRemotePath());
    }

    @Test
    public void testIncBackupFileCreation() throws IOException {
        File file =
                createFile(
                        "target/data/Keyspace1/Standard1/backups/Keyspace1-Standard1-ia-5-Data.db");
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(file, BackupFileType.SST_V2);
        Assert.assertEquals(BackupFileType.SST_V2, backupfile.type);
        Assert.assertEquals("Keyspace1", backupfile.keyspace);
        Assert.assertEquals("Standard1", backupfile.columnFamily);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals(region, backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals(
                "casstestbackup/1049_fake-app/1234567/SST_V2/1699206297000/Keyspace1/Standard1/SNAPPY/PLAINTEXT/Keyspace1-Standard1-ia-5-Data.db",
                backupfile.getRemotePath());
    }

    @Test
    public void testMetaFileCreation() throws IOException {
        File file = createFile("target/data/1234567.meta");
        RemoteBackupPath backupfile = injector.getInstance(RemoteBackupPath.class);
        backupfile.parseLocal(file, BackupFileType.META_V2);
        Assert.assertEquals(BackupFileType.META_V2, backupfile.type);
        Assert.assertEquals("1234567", backupfile.token);
        Assert.assertEquals("fake-app", backupfile.clusterName);
        Assert.assertEquals(region, backupfile.region);
        Assert.assertEquals("casstestbackup", backupfile.baseDir);
        Assert.assertEquals(
                "casstestbackup/1049_fake-app/1234567/META_V2/1699206297000/SNAPPY/PLAINTEXT/1234567.meta",
                backupfile.getRemotePath());
    }

    private File createFile(String path) throws IOException {
        File file = new File(path);
        File dir = file.getParentFile();
        dir.mkdirs();
        file.createNewFile();
        file.setLastModified(1699206297000L);
        return file;
    }
}
