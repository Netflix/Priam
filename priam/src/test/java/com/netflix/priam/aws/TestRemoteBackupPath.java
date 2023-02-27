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

package com.netflix.priam.aws;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cryptography.CryptographyAlgorithm;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import javax.inject.Provider;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 11/23/18. */
public class TestRemoteBackupPath {
    private static final Logger logger = LoggerFactory.getLogger(TestRemoteBackupPath.class);
    private Provider<AbstractBackupPath> pathFactory;
    private IConfiguration configuration;

    public TestRemoteBackupPath() {
        Injector injector = Guice.createInjector(new BRTestModule());
        pathFactory = injector.getProvider(AbstractBackupPath.class);
        configuration = injector.getInstance(IConfiguration.class);
    }

    @Test
    public void testV1BackupPathsSST() throws ParseException {
        Path path =
                Paths.get(
                        configuration.getDataFileLocation(),
                        "keyspace1",
                        "columnfamily1",
                        "backup",
                        "mc-1234-Data.db");
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        abstractBackupPath.parseLocal(path.toFile(), BackupFileType.SST);

        // Verify parse local
        Assert.assertEquals(
                0, abstractBackupPath.getLastModified().toEpochMilli()); // File do not exist.
        Assert.assertEquals("keyspace1", abstractBackupPath.getKeyspace());
        Assert.assertEquals("columnfamily1", abstractBackupPath.getColumnFamily());
        Assert.assertEquals(BackupFileType.SST, abstractBackupPath.getType());
        Assert.assertEquals(path.toFile(), abstractBackupPath.getBackupFile());
        Assert.assertEquals(
                0,
                abstractBackupPath
                        .getTime()
                        .toInstant()
                        .toEpochMilli()); // Since file do not exist.

        // Verify toRemote and parseRemote.
        String remotePath = abstractBackupPath.getRemotePath();
        logger.info(remotePath);
        AbstractBackupPath abstractBackupPath2 = pathFactory.get();
        abstractBackupPath2.parseRemote(remotePath);
        validateAbstractBackupPath(abstractBackupPath, abstractBackupPath2);
        Assert.assertEquals(abstractBackupPath.getTime(), abstractBackupPath2.getTime());
    }

    private void validateAbstractBackupPath(AbstractBackupPath abp1, AbstractBackupPath abp2) {
        Assert.assertEquals(abp1.getKeyspace(), abp2.getKeyspace());
        Assert.assertEquals(abp1.getColumnFamily(), abp2.getColumnFamily());
        Assert.assertEquals(abp1.getFileName(), abp2.getFileName());
        Assert.assertEquals(abp1.getType(), abp2.getType());
        Assert.assertEquals(abp1.getCompression(), abp2.getCompression());
        Assert.assertEquals(abp1.getEncryption(), abp2.getEncryption());
    }

    @Test
    public void testV1BackupPathsSnap() throws ParseException {
        Path path =
                Paths.get(
                        configuration.getDataFileLocation(),
                        "keyspace1",
                        "columnfamily1",
                        "snapshot",
                        "201801011201",
                        "mc-1234-Data.db");
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        abstractBackupPath.parseLocal(path.toFile(), BackupFileType.SNAP);

        // Verify parse local
        Assert.assertEquals(
                0, abstractBackupPath.getLastModified().toEpochMilli()); // File do not exist.
        Assert.assertEquals("keyspace1", abstractBackupPath.getKeyspace());
        Assert.assertEquals("columnfamily1", abstractBackupPath.getColumnFamily());
        Assert.assertEquals(BackupFileType.SNAP, abstractBackupPath.getType());
        Assert.assertEquals(path.toFile(), abstractBackupPath.getBackupFile());
        Assert.assertEquals(
                "201801011201", DateUtil.formatyyyyMMddHHmm(abstractBackupPath.getTime()));

        // Verify toRemote and parseRemote.
        String remotePath = abstractBackupPath.getRemotePath();
        logger.info(remotePath);

        AbstractBackupPath abstractBackupPath2 = pathFactory.get();
        abstractBackupPath2.parseRemote(remotePath);
        validateAbstractBackupPath(abstractBackupPath, abstractBackupPath2);
        Assert.assertEquals(abstractBackupPath.getTime(), abstractBackupPath2.getTime());
    }

    @Test
    public void testV1BackupPathsMeta() throws ParseException {
        Path path = Paths.get(configuration.getDataFileLocation(), "meta.json");
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        abstractBackupPath.parseLocal(path.toFile(), BackupFileType.META);

        // Verify parse local
        Assert.assertEquals(
                0, abstractBackupPath.getLastModified().toEpochMilli()); // File do not exist.
        Assert.assertEquals(null, abstractBackupPath.getKeyspace());
        Assert.assertEquals(null, abstractBackupPath.getColumnFamily());
        Assert.assertEquals(BackupFileType.META, abstractBackupPath.getType());
        Assert.assertEquals(path.toFile(), abstractBackupPath.getBackupFile());

        // Verify toRemote and parseRemote.
        String remotePath = abstractBackupPath.getRemotePath();
        logger.info(remotePath);

        AbstractBackupPath abstractBackupPath2 = pathFactory.get();
        abstractBackupPath2.parseRemote(remotePath);
        validateAbstractBackupPath(abstractBackupPath, abstractBackupPath2);
        Assert.assertEquals(abstractBackupPath.getTime(), abstractBackupPath2.getTime());
    }

    @Test
    public void testV2BackupPathSST() throws ParseException {
        Path path =
                Paths.get(
                        configuration.getDataFileLocation(),
                        "keyspace1",
                        "columnfamily1",
                        "backup",
                        "mc-1234-Data.db");
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        abstractBackupPath.parseLocal(path.toFile(), BackupFileType.SST_V2);

        // Verify parse local
        Assert.assertEquals(
                0, abstractBackupPath.getLastModified().toEpochMilli()); // File do not exist.
        Assert.assertEquals("keyspace1", abstractBackupPath.getKeyspace());
        Assert.assertEquals("columnfamily1", abstractBackupPath.getColumnFamily());
        Assert.assertEquals("SNAPPY", abstractBackupPath.getCompression().name());
        Assert.assertEquals(BackupFileType.SST_V2, abstractBackupPath.getType());
        Assert.assertEquals(path.toFile(), abstractBackupPath.getBackupFile());

        // Verify toRemote and parseRemote.
        Instant now = DateUtil.getInstant();
        abstractBackupPath.setLastModified(now);
        String remotePath = abstractBackupPath.getRemotePath();
        logger.info(remotePath);

        AbstractBackupPath abstractBackupPath2 = pathFactory.get();
        abstractBackupPath2.parseRemote(remotePath);
        Assert.assertEquals(
                now.toEpochMilli() / 1_000L * 1_000L,
                abstractBackupPath2.getLastModified().toEpochMilli());
        validateAbstractBackupPath(abstractBackupPath, abstractBackupPath2);
    }

    @Test
    public void testV2BackupPathMeta() throws ParseException {
        Path path = Paths.get(configuration.getDataFileLocation(), "meta_v2_201801011201.json");
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        abstractBackupPath.parseLocal(path.toFile(), BackupFileType.META_V2);

        // Verify parse local
        Assert.assertEquals(
                0, abstractBackupPath.getLastModified().toEpochMilli()); // File do not exist.
        Assert.assertEquals(null, abstractBackupPath.getKeyspace());
        Assert.assertEquals(null, abstractBackupPath.getColumnFamily());
        Assert.assertEquals(BackupFileType.META_V2, abstractBackupPath.getType());
        Assert.assertEquals(path.toFile(), abstractBackupPath.getBackupFile());
        Assert.assertEquals(CryptographyAlgorithm.PLAINTEXT, abstractBackupPath.getEncryption());

        // Verify toRemote and parseRemote.
        Instant now = DateUtil.getInstant();
        abstractBackupPath.setLastModified(now);
        String remotePath = abstractBackupPath.getRemotePath();
        logger.info(remotePath);

        Assert.assertEquals("SNAPPY", abstractBackupPath.getCompression().name());

        AbstractBackupPath abstractBackupPath2 = pathFactory.get();
        abstractBackupPath2.parseRemote(remotePath);
        Assert.assertEquals(
                now.toEpochMilli() / 1_000L * 1_000L,
                abstractBackupPath2.getLastModified().toEpochMilli());
        validateAbstractBackupPath(abstractBackupPath, abstractBackupPath2);
    }

    @Test
    public void testRemoteV2Prefix() throws ParseException {
        Path path = Paths.get("test_backup");
        AbstractBackupPath abstractBackupPath = pathFactory.get();
        Assert.assertEquals(
                "casstestbackup/1049_fake-app/1808575600/META_V2",
                abstractBackupPath.remoteV2Prefix(path, BackupFileType.META_V2).toString());

        path = Paths.get("s3-bucket-name", "fake_base_dir", "-6717_random_fake_app");
        Assert.assertEquals(
                "fake_base_dir/-6717_random_fake_app/1808575600/META_V2",
                abstractBackupPath.remoteV2Prefix(path, BackupFileType.META_V2).toString());
    }
}
