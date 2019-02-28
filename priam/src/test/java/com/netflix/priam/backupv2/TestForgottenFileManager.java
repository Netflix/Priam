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
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.utils.DateUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by aagrawal on 1/1/19. */
public class TestForgottenFileManager {
    private ForgottenFilesManager forgottenFilesManager;
    private TestBackupUtils testBackupUtils;
    private ForgottenFilesConfiguration configuration;
    private List<Path> allFiles = new ArrayList<>();
    private Instant snapshotInstant;
    private Path snapshotDir;

    public TestForgottenFileManager() {
        Injector injector = Guice.createInjector(new BRTestModule());
        BackupMetrics backupMetrics = injector.getInstance(BackupMetrics.class);
        configuration = new ForgottenFilesConfiguration();
        forgottenFilesManager = new ForgottenFilesManager(configuration, backupMetrics);
        testBackupUtils = injector.getInstance(TestBackupUtils.class);
    }

    @Before
    public void prep() throws Exception {
        cleanup();
        Instant now = DateUtil.getInstant();
        snapshotInstant = now;
        Path file1 = Paths.get(testBackupUtils.createFile("file1", now.minus(10, ChronoUnit.DAYS)));
        Path file2 = Paths.get(testBackupUtils.createFile("file2", now.minus(8, ChronoUnit.DAYS)));
        Path file3 = Paths.get(testBackupUtils.createFile("file3", now.minus(6, ChronoUnit.DAYS)));
        Path file4 = Paths.get(testBackupUtils.createFile("file4", now.minus(4, ChronoUnit.DAYS)));
        Path file5 = Paths.get(testBackupUtils.createFile("file5", now.minus(1, ChronoUnit.DAYS)));
        Path file6 =
                Paths.get(
                        testBackupUtils.createFile(
                                "tmplink-lb-59516-big-Index.db", now.minus(3, ChronoUnit.DAYS)));
        Path file7 =
                Paths.get(testBackupUtils.createFile("file7.tmp", now.minus(3, ChronoUnit.DAYS)));

        allFiles.add(file1);
        allFiles.add(file2);
        allFiles.add(file3);
        allFiles.add(file4);
        allFiles.add(file5);
        allFiles.add(file6);
        allFiles.add(file7);

        // Create a snapshot with file2, file3, file4.
        Path columnfamilyDir = file1.getParent();
        snapshotDir =
                Paths.get(
                        columnfamilyDir.toString(),
                        "snapshot",
                        "snap_v2_" + DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, now));
        snapshotDir.toFile().mkdirs();
        Files.createLink(Paths.get(snapshotDir.toString(), file2.getFileName().toString()), file2);
        Files.createLink(Paths.get(snapshotDir.toString(), file3.getFileName().toString()), file3);
        Files.createLink(Paths.get(snapshotDir.toString(), file4.getFileName().toString()), file4);
    }

    @After
    public void cleanup() throws Exception {
        String dataDir = configuration.getDataFileLocation();
        org.apache.commons.io.FileUtils.cleanDirectory(new File(dataDir));
    }

    @Test
    public void testMoveForgottenFiles() throws IOException, InterruptedException {
        Collection<File> files = allFiles.stream().map(Path::toFile).collect(Collectors.toList());
        Path lostFoundDir =
                Paths.get(configuration.getDataFileLocation(), forgottenFilesManager.LOST_FOUND);

        // Lets create some extra symlinks in the LOST_FOUND folder. They should not exist anymore
        Path randomSymlink = Paths.get(lostFoundDir.toFile().getAbsolutePath(), "random");
        Files.createDirectory(lostFoundDir);
        Files.createSymbolicLink(randomSymlink, lostFoundDir);

        forgottenFilesManager.moveForgottenFiles(
                new File(configuration.getDataFileLocation()), files);

        // Extra symlinks are deleted.
        Assert.assertFalse(Files.exists(randomSymlink));

        // Symlinks are created for all the files. They are not moved yet.
        Collection<File> symlinkFiles = FileUtils.listFiles(lostFoundDir.toFile(), null, false);
        Assert.assertEquals(allFiles.size(), symlinkFiles.size());
        for (Path file : allFiles) {
            Path symlink = Paths.get(lostFoundDir.toString(), file.getFileName().toString());
            Assert.assertTrue(symlinkFiles.contains(symlink.toFile()));
            Assert.assertTrue(Files.isSymbolicLink(symlink));
            Assert.assertTrue(Files.exists(file));
        }

        // Lets change the configuration and try again!!
        configuration.setGracePeriodForgottenFileInDaysForRead(0);
        forgottenFilesManager.moveForgottenFiles(
                new File(configuration.getDataFileLocation()), files);
        Collection<File> movedFiles = FileUtils.listFiles(lostFoundDir.toFile(), null, false);
        Assert.assertEquals(allFiles.size(), movedFiles.size());
        movedFiles
                .stream()
                .forEach(
                        file -> {
                            Assert.assertTrue(
                                    Files.isRegularFile(Paths.get(file.getAbsolutePath())));
                        });
        allFiles.stream()
                .forEach(
                        file -> {
                            Assert.assertFalse(file.toFile().exists());
                        });

        configuration.setGracePeriodForgottenFileInDaysForRead(
                ForgottenFilesConfiguration.DEFAULT_GRACE_PERIOD);
    }

    @Test
    public void getColumnfamilyFiles() {

        Path columnfamilyDir = allFiles.get(0).getParent();
        Collection<File> columnfamilyFiles =
                forgottenFilesManager.getColumnfamilyFiles(
                        snapshotInstant, columnfamilyDir.toFile());
        Assert.assertEquals(3, columnfamilyFiles.size());
        Assert.assertTrue(columnfamilyFiles.contains(allFiles.get(0).toFile()));
        Assert.assertTrue(columnfamilyFiles.contains(allFiles.get(1).toFile()));
        Assert.assertTrue(columnfamilyFiles.contains(allFiles.get(2).toFile()));
    }

    @Test
    public void findAndMoveForgottenFiles() {
        Path lostFoundDir =
                Paths.get(allFiles.get(0).getParent().toString(), forgottenFilesManager.LOST_FOUND);
        forgottenFilesManager.findAndMoveForgottenFiles(snapshotInstant, snapshotDir.toFile());

        // Only one potential forgotten file - file1. It will be symlink here.
        Collection<File> movedFiles = FileUtils.listFiles(lostFoundDir.toFile(), null, false);
        Assert.assertEquals(1, movedFiles.size());
        Assert.assertTrue(
                movedFiles
                        .iterator()
                        .next()
                        .getName()
                        .equals(allFiles.get(0).getFileName().toString()));
        Assert.assertTrue(
                Files.isSymbolicLink(Paths.get(movedFiles.iterator().next().getAbsolutePath())));

        // All files still remain in columnfamily dir.
        Collection<File> cfFiles =
                FileUtils.listFiles(new File(allFiles.get(0).getParent().toString()), null, false);
        Assert.assertEquals(allFiles.size(), cfFiles.size());

        // Snapshot is untouched.
        Collection<File> snapshotFiles = FileUtils.listFiles(snapshotDir.toFile(), null, false);
        Assert.assertEquals(3, snapshotFiles.size());

        // Lets change the configuration and try again!!
        configuration.setGracePeriodForgottenFileInDaysForRead(0);
        forgottenFilesManager.findAndMoveForgottenFiles(snapshotInstant, snapshotDir.toFile());
        configuration.setGracePeriodForgottenFileInDaysForRead(
                ForgottenFilesConfiguration.DEFAULT_GRACE_PERIOD);
        movedFiles = FileUtils.listFiles(lostFoundDir.toFile(), null, false);
        Assert.assertEquals(1, movedFiles.size());
        Assert.assertTrue(
                Files.isRegularFile(Paths.get(movedFiles.iterator().next().getAbsolutePath())));
        cfFiles =
                FileUtils.listFiles(new File(allFiles.get(0).getParent().toString()), null, false);
        Assert.assertEquals(6, cfFiles.size());
        int temp_file_name = 1;
        for (File file : cfFiles) {
            file.getName().equals(allFiles.get(temp_file_name++).getFileName().toString());
        }

        // Snapshot is untouched.
        snapshotFiles = FileUtils.listFiles(snapshotDir.toFile(), null, false);
        Assert.assertEquals(3, snapshotFiles.size());
    }

    private class ForgottenFilesConfiguration extends FakeConfiguration {
        protected static final int DEFAULT_GRACE_PERIOD = 3;
        private int gracePeriodForgottenFileInDaysForRead = DEFAULT_GRACE_PERIOD;

        @Override
        public boolean isForgottenFileMoveEnabled() {
            return true;
        }

        @Override
        public int getForgottenFileGracePeriodDaysForRead() {
            return gracePeriodForgottenFileInDaysForRead;
        }

        public void setGracePeriodForgottenFileInDaysForRead(
                int gracePeriodForgottenFileInDaysForRead) {
            this.gracePeriodForgottenFileInDaysForRead = gracePeriodForgottenFileInDaysForRead;
        }
    }
}
