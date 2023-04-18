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

package com.netflix.priam.backupv2;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.FakeBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by aagrawal on 12/5/18. */
public class TestMetaV2Proxy {
    private FakeBackupFileSystem fs;
    private IConfiguration configuration;
    private TestBackupUtils backupUtils;
    private IMetaProxy metaProxy;
    private Provider<AbstractBackupPath> abstractBackupPathProvider;

    public TestMetaV2Proxy() {
        Injector injector = Guice.createInjector(new BRTestModule());
        configuration = injector.getInstance(IConfiguration.class);
        fs = injector.getInstance(FakeBackupFileSystem.class);
        fs.setupTest(getRemoteFakeFiles());
        backupUtils = new TestBackupUtils();
        metaProxy = injector.getInstance(MetaV2Proxy.class);
        abstractBackupPathProvider = injector.getProvider(AbstractBackupPath.class);
    }

    @Before
    public void setUp() {
        new File(configuration.getDataFileLocation()).mkdirs();
    }

    @Test
    public void testMetaPrefix() {
        // Null date range
        Assert.assertEquals(getPrefix() + "/META_V2", metaProxy.getMetaPrefix(null));
        Instant now = Instant.now();
        // No end date.
        Assert.assertEquals(
                getPrefix() + "/META_V2/" + now.toEpochMilli(),
                metaProxy.getMetaPrefix(new DateUtil.DateRange(now, null)));
        // No start date
        Assert.assertEquals(
                getPrefix() + "/META_V2",
                metaProxy.getMetaPrefix(new DateUtil.DateRange(null, Instant.now())));
        long start = 1834567890L;
        long end = 1834877776L;
        Assert.assertEquals(
                getPrefix() + "/META_V2/1834",
                metaProxy.getMetaPrefix(
                        new DateUtil.DateRange(
                                Instant.ofEpochSecond(start), Instant.ofEpochSecond(end))));
    }

    @Test
    public void testIsMetaFileValid() throws Exception {
        Instant snapshotInstant = DateUtil.getInstant();
        Path metaPath = backupUtils.createMeta(getRemoteFakeFiles(), snapshotInstant);
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(metaPath.toFile(), AbstractBackupPath.BackupFileType.META_V2);

        Assert.assertTrue(metaProxy.isMetaFileValid(abstractBackupPath).valid);
        FileUtils.deleteQuietly(metaPath.toFile());

        List<String> fileToAdd = getRemoteFakeFiles();
        fileToAdd.add(
                Paths.get(
                                getPrefix(),
                                AbstractBackupPath.BackupFileType.SST_V2.toString(),
                                "1859817645000",
                                "keyspace1",
                                "columnfamily1",
                                "SNAPPY",
                                "PLAINTEXT",
                                "file9.Data.db")
                        .toString());

        metaPath = backupUtils.createMeta(fileToAdd, snapshotInstant);
        Assert.assertFalse(metaProxy.isMetaFileValid(abstractBackupPath).valid);
        FileUtils.deleteQuietly(metaPath.toFile());

        metaPath = Paths.get(configuration.getDataFileLocation(), "meta_v2_201801010000.json");
        Assert.assertFalse(metaProxy.isMetaFileValid(abstractBackupPath).valid);
    }

    @Test
    public void testGetSSTFilesFromMeta() throws Exception {
        Instant snapshotInstant = DateUtil.getInstant();
        List<String> remoteFiles = getRemoteFakeFiles();
        Path metaPath = backupUtils.createMeta(remoteFiles, snapshotInstant);
        List<String> filesFromMeta = metaProxy.getSSTFilesFromMeta(metaPath);
        filesFromMeta.removeAll(remoteFiles);
        Assert.assertTrue(filesFromMeta.isEmpty());
    }

    @Test
    public void testGetIncrementalFiles() throws Exception {
        DateUtil.DateRange dateRange = new DateUtil.DateRange("202812071820,20281229");
        Iterator<AbstractBackupPath> incrementals = metaProxy.getIncrementals(dateRange);
        int i = 0;
        while (incrementals.hasNext()) {
            System.out.println(incrementals.next());
            i++;
        }
        Assert.assertEquals(3, i);
    }

    @Test
    public void testFindMetaFiles() throws BackupRestoreException {
        List<AbstractBackupPath> metas =
                metaProxy.findMetaFiles(
                        new DateUtil.DateRange(
                                Instant.ofEpochMilli(1859824860000L),
                                Instant.ofEpochMilli(1859828420000L)));
        Assert.assertEquals(1, metas.size());
        Assert.assertEquals("meta_v2_202812071801.json", metas.get(0).getFileName());
        Assert.assertTrue(fs.doesRemoteFileExist(Paths.get(metas.get(0).getRemotePath())));

        metas =
                metaProxy.findMetaFiles(
                        new DateUtil.DateRange(
                                Instant.ofEpochMilli(1859824860000L),
                                Instant.ofEpochMilli(1859828460000L)));
        Assert.assertEquals(2, metas.size());
    }

    @Test
    public void testFindLatestValidMetaFile() {}

    private String getPrefix() {
        return "casstestbackup/1049_fake-app/1808575600";
    }

    private List<String> getRemoteFakeFiles() {
        List<Path> files = new ArrayList<>();
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859817645000",
                        "keyspace1",
                        "columnfamily1",
                        "SNAPPY",
                        "PLAINTEXT",
                        "file1-Data.db"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859818845000",
                        "keyspace1",
                        "columnfamily1",
                        "SNAPPY",
                        "PLAINTEXT",
                        "file2-Data.db"));

        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.META_V2.toString(),
                        "1859824860000",
                        "SNAPPY",
                        "PLAINTEXT",
                        "meta_v2_202812071801.json"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859826045000",
                        "keyspace1",
                        "columnfamily1",
                        "SNAPPY",
                        "PLAINTEXT",
                        "manifest"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859828410000",
                        "keyspace1",
                        "columnfamily1",
                        "SNAPPY",
                        "PLAINTEXT",
                        "file3-Data.db"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859828420000",
                        "keyspace1",
                        "columnfamily1",
                        "SNAPPY",
                        "PLAINTEXT",
                        "file4-Data.db"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.META_V2.toString(),
                        "1859828460000",
                        "SNAPPY",
                        "PLAINTEXT",
                        "meta_v2_202812071901.json"));
        return files.stream().map(Path::toString).collect(Collectors.toList());
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.cleanDirectory(new File(configuration.getDataFileLocation()));
    }

    @Test
    public void testCleanupOldMetaFiles() throws IOException {
        generateDummyMetaFiles();
        Path dataDir = Paths.get(configuration.getDataFileLocation());
        Assert.assertEquals(4, dataDir.toFile().listFiles().length);

        // clean the directory
        metaProxy.cleanupOldMetaFiles();

        Assert.assertEquals(1, dataDir.toFile().listFiles().length);
        Path dummy = Paths.get(dataDir.toString(), "dummy.tmp");
        Assert.assertTrue(dummy.toFile().exists());
    }

    private void generateDummyMetaFiles() throws IOException {
        Path dataDir = Paths.get(configuration.getDataFileLocation());
        FileUtils.cleanDirectory(dataDir.toFile());
        FileUtils.write(
                Paths.get(
                                configuration.getDataFileLocation(),
                                MetaFileInfo.getMetaFileName(DateUtil.getInstant()))
                        .toFile(),
                "dummy",
                "UTF-8");

        FileUtils.write(
                Paths.get(
                                configuration.getDataFileLocation(),
                                MetaFileInfo.getMetaFileName(
                                        DateUtil.getInstant().minus(10, ChronoUnit.MINUTES)))
                        .toFile(),
                "dummy",
                "UTF-8");

        FileUtils.write(
                Paths.get(
                                configuration.getDataFileLocation(),
                                MetaFileInfo.getMetaFileName(DateUtil.getInstant()) + ".tmp")
                        .toFile(),
                "dummy",
                "UTF-8");

        FileUtils.write(
                Paths.get(configuration.getDataFileLocation(), "dummy.tmp").toFile(),
                "dummy",
                "UTF-8");
    }
}
