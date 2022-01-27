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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.FakeBackupFileSystem;
import com.netflix.priam.backup.Status;
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
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        // list.add(getRemoteFromLocal(file6));
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

    @AfterEach
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
        Assertions.assertEquals(8, remoteFiles.size());
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-4-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-5-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-6-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-7-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-1-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("META1")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("META2")));
        // Remains because of GRACE PERIOD.
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-3-Data.db")));

        Assertions.assertFalse(remoteFiles.contains(allFilesMap.get("mc-2-Data.db")));
        Assertions.assertFalse(remoteFiles.contains(allFilesMap.get("META0")));
    }

    @Test
    public void testTTLNext() throws Exception {
        int daysForSnapshot = configuration.getBackupRetentionDays() + 1;
        prepTest(daysForSnapshot);
        // Run ttl till 3rd meta file.
        backupTTLService.execute();

        List<String> remoteFiles = getAllFiles();
        // Confirm the files.
        Assertions.assertEquals(6, remoteFiles.size());
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-4-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-6-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-7-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("META2")));
        // GRACE PERIOD files.
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-3-Data.db")));
        Assertions.assertTrue(remoteFiles.contains(allFilesMap.get("mc-5-Data.db")));

        Assertions.assertFalse(remoteFiles.contains(allFilesMap.get("mc-1-Data.db")));
        Assertions.assertFalse(remoteFiles.contains(allFilesMap.get("mc-2-Data.db")));
        Assertions.assertFalse(remoteFiles.contains(allFilesMap.get("META0")));
        Assertions.assertFalse(remoteFiles.contains(allFilesMap.get("META1")));
    }

    @Test
    public void testRestoreMode(@Mocked InstanceState state) throws Exception {
        new Expectations() {
            {
                state.getRestoreStatus().getStatus();
                result = Status.STARTED;
            }
        };
        backupTTLService.execute();
    }
}
