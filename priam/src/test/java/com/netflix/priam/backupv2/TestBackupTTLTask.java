/*
 * Copyright 2019 Netflix, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.*;
import com.netflix.priam.config.BackupsToCompress;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.utils.BackupFileUtils;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Provider;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/** Created by aagrawal on 12/17/18. */
public class TestBackupTTLTask {

    private TestBackupUtils testBackupUtils = new TestBackupUtils();
    private IConfiguration configuration;
    private static BackupTTLTask backupTTLService;
    private static FakeBackupFileSystem backupFileSystem;
    private Provider<AbstractBackupPath> pathProvider;
    private Path[] metas;
    private Map<String, String> allFilesMap = new HashMap<>();

    public TestBackupTTLTask() {
        Injector injector = Guice.createInjector(new BRTestModule());
        configuration = injector.getInstance(IConfiguration.class);
        if (backupTTLService == null) backupTTLService = injector.getInstance(BackupTTLTask.class);
        if (backupFileSystem == null)
            backupFileSystem = injector.getInstance(FakeBackupFileSystem.class);
        pathProvider = injector.getProvider(AbstractBackupPath.class);
    }

    public void prepTest(int daysForSnapshot) throws Exception {
        BackupFileUtils.cleanupDir(Paths.get(configuration.getDataFileLocation()));
        Instant current = DateUtil.getInstant();

        List<String> list = new ArrayList<>();
        List<String> allFiles = new ArrayList<>();
        metas = new Path[3];

        Instant time =
                current.minus(
                        daysForSnapshot + configuration.getGracePeriodDaysForCompaction() + 1,
                        ChronoUnit.DAYS);
        String file1 = testBackupUtils.createFile("mc-1-Data.db", time);
        String file2 =
                testBackupUtils.createFile("mc-2-Data.db", time.plus(10, ChronoUnit.MINUTES));
        list.clear();
        list.add(getRemoteFromLocal(file1));
        list.add(getRemoteFromLocal(file2));
        metas[0] = testBackupUtils.createMeta(list, time.plus(20, ChronoUnit.MINUTES));
        allFiles.add(getRemoteFromLocal(file1));
        allFiles.add(getRemoteFromLocal(file2));

        time = current.minus(daysForSnapshot, ChronoUnit.DAYS);
        String file3 = testBackupUtils.createFile("mc-3-Data.db", time);
        String file4 =
                testBackupUtils.createFile("mc-4-Data.db", time.plus(10, ChronoUnit.MINUTES));
        list.clear();
        list.add(getRemoteFromLocal(file1));
        list.add(getRemoteFromLocal(file4));
        metas[1] = testBackupUtils.createMeta(list, time.plus(20, ChronoUnit.MINUTES));
        allFiles.add(getRemoteFromLocal(file3));
        allFiles.add(getRemoteFromLocal(file4));

        time = current.minus(daysForSnapshot - 1, ChronoUnit.DAYS);
        String file5 = testBackupUtils.createFile("mc-5-Data.db", time);
        String file6 =
                testBackupUtils.createFile("mc-6-Data.db", time.plus(10, ChronoUnit.MINUTES));
        String file7 =
                testBackupUtils.createFile("mc-7-Data.db", time.plus(20, ChronoUnit.MINUTES));
        list.clear();
        list.add(getRemoteFromLocal(file4));
        list.add(getRemoteFromLocal(file7));
        metas[2] = testBackupUtils.createMeta(list, time.plus(40, ChronoUnit.MINUTES));
        allFiles.add(getRemoteFromLocal(file5));
        allFiles.add(getRemoteFromLocal(file6));
        allFiles.add(getRemoteFromLocal(file7));

        allFiles.stream()
                .forEach(
                        file -> {
                            Path path = Paths.get(file);
                            allFilesMap.put(path.toFile().getName(), file);
                        });

        for (int i = 0; i < metas.length; i++) {
            AbstractBackupPath path = pathProvider.get();
            path.parseLocal(metas[i].toFile(), AbstractBackupPath.BackupFileType.META_V2);
            allFiles.add(path.getRemotePath());
            allFilesMap.put("META" + i, path.getRemotePath());
        }

        backupFileSystem.setupTest(allFiles);
    }

    private String getRemoteFromLocal(String localPath) throws ParseException {
        AbstractBackupPath path = pathProvider.get();
        path.parseLocal(new File(localPath), AbstractBackupPath.BackupFileType.SST_V2);
        return path.getRemotePath();
    }

    @After
    public void cleanup() {
        BackupFileUtils.cleanupDir(Paths.get(configuration.getDataFileLocation()));
        backupFileSystem.cleanup();
    }

    private List<String> getAllFiles() {
        List<String> remoteFiles = new ArrayList<>();
        backupFileSystem.listFileSystem("", null, null).forEachRemaining(remoteFiles::add);
        return remoteFiles;
    }

    @Test
    public void testTTL() throws Exception {
        int daysForSnapshot = configuration.getBackupRetentionDays();
        prepTest(daysForSnapshot);
        // Run ttl till 2nd meta file.
        backupTTLService.execute();

        List<String> remoteFiles = getAllFiles();

        // Confirm the files.
        Assert.assertEquals(8, remoteFiles.size());
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-4-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-5-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-6-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-7-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-1-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("META1")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("META2")));
        // Remains because of GRACE PERIOD.
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-3-Data.db")));

        Assert.assertFalse(remoteFiles.contains(allFilesMap.get("mc-2-Data.db")));
        Assert.assertFalse(remoteFiles.contains(allFilesMap.get("META0")));
    }

    @Test
    public void testTTLNext() throws Exception {
        int daysForSnapshot = configuration.getBackupRetentionDays() + 1;
        prepTest(daysForSnapshot);
        // Run ttl till 3rd meta file.
        backupTTLService.execute();

        List<String> remoteFiles = getAllFiles();
        // Confirm the files.
        Assert.assertEquals(6, remoteFiles.size());
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-4-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-6-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-7-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("META2")));
        // GRACE PERIOD files.
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-3-Data.db")));
        Assert.assertTrue(remoteFiles.contains(allFilesMap.get("mc-5-Data.db")));

        Assert.assertFalse(remoteFiles.contains(allFilesMap.get("mc-1-Data.db")));
        Assert.assertFalse(remoteFiles.contains(allFilesMap.get("mc-2-Data.db")));
        Assert.assertFalse(remoteFiles.contains(allFilesMap.get("META0")));
        Assert.assertFalse(remoteFiles.contains(allFilesMap.get("META1")));
    }

    @Test
    public void testRestoreMode(@Mocked InstanceState.RestoreStatus restoreStatus)
            throws Exception {
        new Expectations() {
            {
                restoreStatus.getStatus();
                result = Status.STARTED;
            }
        };
        backupTTLService.execute();
    }

    @Test
    public void testFileIsNotDeletedIfOnlyCompressionIsDifferent() throws Exception {
        BackupFileUtils.cleanupDir(Paths.get(configuration.getDataFileLocation()));
        int lookback =
            configuration.getBackupRetentionDays() + configuration.getGracePeriodDaysForCompaction() + 1;
        Instant time = DateUtil.getInstant().minus(lookback, ChronoUnit.DAYS);
        String file = testBackupUtils.createFile("mc-1-Data.db", time);
        time =
            time.plus(configuration.getGracePeriodDaysForCompaction() + 1, ChronoUnit.DAYS)
                .plus(20, ChronoUnit.MINUTES);
        Path metapath = testBackupUtils.createMeta(ImmutableList.of(getRemoteFromLocal(file)), time);
        AbstractBackupPath meta = pathProvider.get();
        meta.parseLocal(metapath.toFile(), AbstractBackupPath.BackupFileType.META_V2);
        ((FakeConfiguration) configuration).setBackupsToCompress(BackupsToCompress.NONE);
        String data = getRemoteFromLocal(file);
        backupFileSystem.setupTest(ImmutableList.of(data, meta.getRemotePath()));

        backupTTLService.execute();

        Assert.assertTrue(getAllFiles().contains(data));
    }

    @Test
    public void testRemoveCompressionPart_sst_NONE() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/table-9f5c6374d48532299a0a5094af9ad1e3/NONE/PLAINTEXT/nb-10-big-Statistics.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/table-9f5c6374d48532299a0a5094af9ad1e3/PLAINTEXT/nb-10-big-Statistics.db");
    }

    @Test
    public void testRemoveCompressionPart_sst_SNAPPY() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/table-9f5c6374d48532299a0a5094af9ad1e3/SNAPPY/PLAINTEXT/nb-10-big-Statistics.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/table-9f5c6374d48532299a0a5094af9ad1e3/PLAINTEXT/nb-10-big-Statistics.db");
    }

    @Test
    public void testRemoveCompressionPart_sst_unsupportedCompression() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/table-9f5c6374d48532299a0a5094af9ad1e3/LZ4/PLAINTEXT/nb-10-big-Statistics.db"));
    }

    @Test
    public void testRemoveCompressionPart_sst_SNAPPYInKeyspace() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/SNAPPY/table-9f5c6374d48532299a0a5094af9ad1e3/SNAPPY/PLAINTEXT/nb-10-big-Statistics.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/SNAPPY/table-9f5c6374d48532299a0a5094af9ad1e3/PLAINTEXT/nb-10-big-Statistics.db");
    }

    @Test
    public void testRemoveCompressionPart_sst_NONEInKeyspace() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/NONE/table-9f5c6374d48532299a0a5094af9ad1e3/NONE/PLAINTEXT/nb-10-big-Statistics.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/NONE/table-9f5c6374d48532299a0a5094af9ad1e3/PLAINTEXT/nb-10-big-Statistics.db");
    }

    @Test
    public void testRemoveCompressionPart_sst_SNAPPYInTable() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/SNAPPY-9f5c6374d48532299a0a5094af9ad1e3/SNAPPY/PLAINTEXT/nb-10-big-Statistics.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/SNAPPY-9f5c6374d48532299a0a5094af9ad1e3/PLAINTEXT/nb-10-big-Statistics.db");
    }

    @Test
    public void testRemoveCompressionPart_sst_NONEInTable() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/NONE-9f5c6374d48532299a0a5094af9ad1e3/NONE/PLAINTEXT/nb-10-big-Statistics.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SST_V2/1724992379000/keyspace/NONE-9f5c6374d48532299a0a5094af9ad1e3/PLAINTEXT/nb-10-big-Statistics.db");
    }

    @Test
    public void testRemoveCompressionPart_si_NONE() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/NONE/PLAINTEXT/keyspace-table.table_field_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/PLAINTEXT/keyspace-table.table_field_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_SNAPPY() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/SNAPPY/PLAINTEXT/keyspace-table.table_field_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/PLAINTEXT/keyspace-table.table_field_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_unsupportedCompression() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/LZ4/PLAINTEXT/keyspace-table.table_field_idx-ka-1-CRC.db"));
    }

    @Test
    public void testRemoveCompressionPart_si_SNAPPYInKeyspace() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/SNAPPY/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/SNAPPY/PLAINTEXT/SNAPPY-table.table_field_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/SNAPPY/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/PLAINTEXT/SNAPPY-table.table_field_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_NONEInKeyspace() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/NONE/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/NONE/PLAINTEXT/NONE-table.table_field_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/NONE/table-672cc1f038a311e68d78cd36d8a9052a/.table_field_idx/PLAINTEXT/NONE-table.table_field_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_SNAPPYInTable() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/SNAPPY-672cc1f038a311e68d78cd36d8a9052a/.SNAPPY_field_idx/SNAPPY/PLAINTEXT/keyspace-SNAPPY.SNAPPY_field_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/SNAPPY-672cc1f038a311e68d78cd36d8a9052a/.SNAPPY_field_idx/PLAINTEXT/keyspace-SNAPPY.SNAPPY_field_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_NONEInTable() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/NONE-672cc1f038a311e68d78cd36d8a9052a/.NONE_field_idx/NONE/PLAINTEXT/keyspace-NONE.NONE_field_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/NONE-672cc1f038a311e68d78cd36d8a9052a/.NONE_field_idx/PLAINTEXT/keyspace-NONE.NONE_field_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_SNAPPYInIndex() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_SNAPPY_idx/SNAPPY/PLAINTEXT/keyspace-table.table_SNAPPY_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_SNAPPY_idx/PLAINTEXT/keyspace-table.table_SNAPPY_idx-ka-1-CRC.db");
    }

    @Test
    public void testRemoveCompressionPart_si_NONEInIndex() {
        Truth.assertThat(BackupTTLTask.removeCompressionPart("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_NONE_idx/NONE/PLAINTEXT/keyspace-table.table_NONE_idx-ka-1-CRC.db"))
            .isEqualTo("test_backup/1234_cass_foo/-4419532432517671141/SECONDARY_INDEX_V2/1724992379000/keyspace/table-672cc1f038a311e68d78cd36d8a9052a/.table_NONE_idx/PLAINTEXT/keyspace-table.table_NONE_idx-ka-1-CRC.db");
    }
}
