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
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.FakeBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.utils.DateUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 12/5/18. */
public class TestBackupValidator {
    private FakeBackupFileSystem fs;
    private Provider<AbstractBackupPath> pathProvider;
    private IConfiguration configuration;
    private BackupValidator backupValidator;
    private MetaFileWriterBuilder metaFileWriterBuilder;

    public TestBackupValidator() {
        Injector injector = Guice.createInjector(new BRTestModule());
        pathProvider = injector.getProvider(AbstractBackupPath.class);
        configuration = injector.getInstance(IConfiguration.class);
        fs = injector.getInstance(FakeBackupFileSystem.class);
        fs.setupTest(getFakeFiles());
        backupValidator = injector.getInstance(BackupValidator.class);
        metaFileWriterBuilder = injector.getInstance(MetaFileWriterBuilder.class);
    }

    @Test
    public void testMetaPrefix() {
        // Null date range
        Assert.assertEquals(getPrefix() + "/META_V2", backupValidator.getMetaPrefix(null));
        // No end date.
        Assert.assertEquals(
                getPrefix() + "/META_V2",
                backupValidator.getMetaPrefix(new DateUtil.DateRange(Instant.now(), null)));
        // No start date
        Assert.assertEquals(
                getPrefix() + "/META_V2",
                backupValidator.getMetaPrefix(new DateUtil.DateRange(null, Instant.now())));
        long start = 1834567890L;
        long end = 1834877776L;
        Assert.assertEquals(
                getPrefix() + "/META_V2/1834",
                backupValidator.getMetaPrefix(
                        new DateUtil.DateRange(
                                Instant.ofEpochSecond(start), Instant.ofEpochSecond(end))));
    }

    @Test
    public void testIsMetaFileValid() throws Exception {
        Path metaPath = createMeta(getFakeFiles());
        Assert.assertTrue(backupValidator.isMetaFileValid(metaPath));
        FileUtils.deleteQuietly(metaPath.toFile());

        List<String> fileToAdd = getFakeFiles();
        fileToAdd.add(
                Paths.get(
                                getPrefix(),
                                AbstractBackupPath.BackupFileType.SST_V2.toString(),
                                "1859817645000",
                                "keyspace1",
                                "columnfamily1",
                                "file9.Data.db.SNAPPY")
                        .toString());

        metaPath = createMeta(fileToAdd);
        Assert.assertFalse(backupValidator.isMetaFileValid(metaPath));
        FileUtils.deleteQuietly(metaPath.toFile());

        metaPath = Paths.get(configuration.getDataFileLocation(), "meta_v2_201901010000.json");
        Assert.assertFalse(backupValidator.isMetaFileValid(metaPath));
    }

    @Test
    public void testFindMetaFiles() throws BackupRestoreException {
        List<AbstractBackupPath> metas =
                backupValidator.findMetaFiles(
                        new DateUtil.DateRange(
                                Instant.ofEpochMilli(1859824860000L),
                                Instant.ofEpochMilli(1859828420000L)));
        Assert.assertEquals(1, metas.size());
        Assert.assertEquals("meta_v2_202812071801.json", metas.get(0).getFileName());
        Assert.assertTrue(fs.doesRemoteFileExist(Paths.get(metas.get(0).getRemotePath())));

        metas =
                backupValidator.findMetaFiles(
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

    private Path createMeta(List<String> filesToAdd) throws IOException {
        MetaFileWriterBuilder.DataStep dataStep =
                metaFileWriterBuilder
                        .newBuilder()
                        .startMetaFileGeneration(Instant.ofEpochMilli(1859824860000L));
        ColumnfamilyResult columnfamilyResult =
                new ColumnfamilyResult("keyspace1", "columnfamily1");
        for (String file : filesToAdd) {
            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(file);
            if (path.getType() == AbstractBackupPath.BackupFileType.SST_V2) {
                ColumnfamilyResult.SSTableResult ssTableResult =
                        new ColumnfamilyResult.SSTableResult();

                ssTableResult.setSstableComponents(new ArrayList<>());
                ssTableResult.getSstableComponents().add(getFileUploadResult(path));
                columnfamilyResult.addSstable(ssTableResult);
            }
        }
        dataStep.addColumnfamilyResult(columnfamilyResult);
        return dataStep.endMetaFileGeneration().getMetaFilePath();
    }

    private FileUploadResult getFileUploadResult(AbstractBackupPath path) {
        FileUploadResult fileUploadResult =
                new FileUploadResult(
                        Paths.get(path.getFileName()),
                        "keyspace1",
                        "columnfamily1",
                        path.getLastModified(),
                        path.getLastModified(),
                        path.getSize());
        fileUploadResult.setBackupPath(path.getRemotePath());
        return fileUploadResult;
    }

    private List<String> getFakeFiles() {
        List<Path> files = new ArrayList<>();
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859817645000",
                        "keyspace1",
                        "columnfamily1",
                        "file1.Data.db.SNAPPY"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859818845000",
                        "keyspace1",
                        "columnfamily1",
                        "file2.Data.db.SNAPPY"));

        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.META_V2.toString(),
                        "1859824860000",
                        "meta_v2_202812071801.json.SNAPPY"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859826045000",
                        "keyspace1",
                        "columnfamily1",
                        "manifest.SNAPPY"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859828410000",
                        "keyspace1",
                        "columnfamily1",
                        "file3.Data.db.SNAPPY"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.SST_V2.toString(),
                        "1859828420000",
                        "keyspace1",
                        "columnfamily1",
                        "file4.Data.db.SNAPPY"));
        files.add(
                Paths.get(
                        getPrefix(),
                        AbstractBackupPath.BackupFileType.META_V2.toString(),
                        "1859828460000",
                        "meta_v2_202812071901.json.SNAPPY"));
        return files.stream().map(Path::toString).collect(Collectors.toList());
    }
}
