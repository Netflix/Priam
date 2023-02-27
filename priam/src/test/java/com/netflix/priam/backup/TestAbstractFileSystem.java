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

package com.netflix.priam.backup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.utils.BackupFileUtils;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The goal of this class is to test common functionality which are encapsulated in
 * AbstractFileSystem. The actual upload/download of a file to remote file system is beyond the
 * scope of this class. Created by aagrawal on 9/22/18.
 */
public class TestAbstractFileSystem {
    private Injector injector;
    private IConfiguration configuration;
    private BackupMetrics backupMetrics;
    private BackupNotificationMgr backupNotificationMgr;
    private FailureFileSystem failureFileSystem;
    private MyFileSystem myFileSystem;

    @Before
    public void setBackupMetrics() {
        if (injector == null) injector = Guice.createInjector(new BRTestModule());

        if (configuration == null) configuration = injector.getInstance(IConfiguration.class);

        if (backupNotificationMgr == null)
            backupNotificationMgr = injector.getInstance(BackupNotificationMgr.class);

        backupMetrics = injector.getInstance(BackupMetrics.class);
        Provider<AbstractBackupPath> pathProvider = injector.getProvider(AbstractBackupPath.class);

        if (failureFileSystem == null)
            failureFileSystem =
                    new FailureFileSystem(
                            configuration, backupMetrics, backupNotificationMgr, pathProvider);

        if (myFileSystem == null)
            myFileSystem =
                    new MyFileSystem(
                            configuration, backupMetrics, backupNotificationMgr, pathProvider);

        BackupFileUtils.cleanupDir(Paths.get(configuration.getDataFileLocation()));
    }

    @Test
    public void testFailedRetriesUpload() throws Exception {
        try {
            Collection<File> files = generateFiles(1, 1, 1);
            for (File file : files) {
                failureFileSystem.uploadAndDelete(getDummyPath(file.toPath()), false /* async */);
            }
        } catch (BackupRestoreException e) {
            // Verify the failure metric for upload is incremented.
            Assert.assertEquals(1, (int) backupMetrics.getInvalidUploads().count());
        }
    }

    private AbstractBackupPath getDummyPath() throws ParseException {
        return getDummyPath(Paths.get(configuration.getDataFileLocation() + "/ks/cf/file-Data.db"));
    }

    private AbstractBackupPath getDummyPath(Path localPath) throws ParseException {
        AbstractBackupPath path = injector.getInstance(AbstractBackupPath.class);
        path.parseLocal(localPath.toFile(), AbstractBackupPath.BackupFileType.SST_V2);
        return path;
    }

    private Collection<File> generateFiles(int noOfKeyspaces, int noOfCf, int noOfSstables)
            throws Exception {
        Path dataDir = Paths.get(configuration.getDataFileLocation());
        BackupFileUtils.generateDummyFiles(
                dataDir, noOfKeyspaces, noOfCf, noOfSstables, "snapshot", "201812310000", true);
        String[] ext = {"db"};
        return FileUtils.listFiles(dataDir.toFile(), ext, true);
    }

    @Test
    public void testFailedRetriesDownload() throws Exception {
        try {
            failureFileSystem.downloadFile(getDummyPath(), "", 2);
        } catch (BackupRestoreException e) {
            // Verify the failure metric for download is incremented.
            Assert.assertEquals(1, (int) backupMetrics.getInvalidDownloads().count());
        }
    }

    @Test
    public void testUpload() throws Exception {
        File file = generateFiles(1, 1, 1).iterator().next();
        myFileSystem.uploadAndDelete(getDummyPath(file.toPath()), false /* async */);
        Assert.assertEquals(1, (int) backupMetrics.getValidUploads().actualCount());
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testDownload() throws Exception {
        // Dummy download
        myFileSystem.downloadFile(getDummyPath(), "", 2);
        // Verify the success metric for download is incremented.
        Assert.assertEquals(1, (int) backupMetrics.getValidDownloads().actualCount());
    }

    @Test
    public void testAsyncUpload() throws Exception {
        File file = generateFiles(1, 1, 1).iterator().next();
        myFileSystem
                .uploadAndDelete(getDummyPath(file.toPath()), Instant.EPOCH, true /* async */)
                .get();
        Assert.assertEquals(1, (int) backupMetrics.getValidUploads().actualCount());
        Assert.assertEquals(0, myFileSystem.getUploadTasksQueued());
    }

    @Test
    public void testAsyncUploadBulk() throws Exception {
        // Testing the queue feature works.
        // 1. Give 1000 dummy files to upload. File upload takes some random time to upload
        Collection<File> files = generateFiles(1, 1, 20);
        List<Future<AbstractBackupPath>> futures = new ArrayList<>();
        for (File file : files) {
            futures.add(
                    myFileSystem.uploadAndDelete(
                            getDummyPath(file.toPath()), Instant.EPOCH, true /* async */));
        }

        // Verify all the work is finished.
        for (Future<AbstractBackupPath> future : futures) {
            future.get();
        }
        // 2. Success metric is incremented correctly
        Assert.assertEquals(files.size(), (int) backupMetrics.getValidUploads().actualCount());

        // 3. The task queue is empty after upload is finished.
        Assert.assertEquals(0, myFileSystem.getUploadTasksQueued());
    }

    @Test
    public void testUploadDedup() throws Exception {
        // Testing the de-duping works.
        Collection<File> files = generateFiles(1, 1, 1);
        File file = files.iterator().next();
        AbstractBackupPath abstractBackupPath = getDummyPath(file.toPath());
        // 1. Give same file to upload x times. Only one request will be entertained.
        int size = 10;
        ExecutorService threads = Executors.newFixedThreadPool(size);
        List<Callable<Boolean>> torun = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            torun.add(
                    () -> {
                        myFileSystem.uploadAndDelete(abstractBackupPath, false /* async */);
                        return Boolean.TRUE;
                    });
        }

        // all tasks executed in different threads, at 'once'.
        List<Future<Boolean>> futures = threads.invokeAll(torun);

        // no more need for the threadpool
        threads.shutdown();
        for (Future<Boolean> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                // Do nothing.
            }
        }
        // 2. Verify the success metric for upload is not same as size, i.e. some amount of
        // de-duping happened.
        Assert.assertNotEquals(size, (int) backupMetrics.getValidUploads().actualCount());
    }

    @Test
    public void testAsyncUploadFailure() throws Exception {
        // Testing single async upload.
        Collection<File> files = generateFiles(1, 1, 1);
        for (File file : files) {
            Future<AbstractBackupPath> future =
                    failureFileSystem.uploadAndDelete(
                            getDummyPath(file.toPath()), Instant.EPOCH, true /* async */);
            try {
                future.get();
            } catch (Exception e) {
                // 1. Future get returns error message.

                // 2. Verify the failure metric for upload is incremented.
                Assert.assertEquals(1, (int) backupMetrics.getInvalidUploads().count());

                // 3. The task queue is empty after upload is finished.
                Assert.assertEquals(0, failureFileSystem.getUploadTasksQueued());
                break;
            }
        }
    }

    @Test
    public void testAsyncDownload() throws Exception {
        // Testing single async download.
        Future<Path> future = myFileSystem.asyncDownloadFile(getDummyPath(), 2);
        future.get();
        // 1. Verify the success metric for download is incremented.
        Assert.assertEquals(1, (int) backupMetrics.getValidDownloads().actualCount());
        // 2. Verify the queue size is '0' after success.
        Assert.assertEquals(0, myFileSystem.getDownloadTasksQueued());
    }

    @Test
    public void testAsyncDownloadBulk() throws Exception {
        // Testing the queue feature works.
        // 1. Give 1000 dummy files to download. File download takes some random time to download.
        int totalFiles = 1000;
        List<Future<Path>> futureList = new ArrayList<>();
        for (int i = 0; i < totalFiles; i++)
            futureList.add(myFileSystem.asyncDownloadFile(getDummyPath(Paths.get("" + i)), 2));

        // Ensure processing is finished.
        for (Future future1 : futureList) {
            future1.get();
        }

        // 2. Success metric is incremented correctly -> exactly 1000 times.
        Assert.assertEquals(totalFiles, (int) backupMetrics.getValidDownloads().actualCount());

        // 3. The task queue is empty after download is finished.
        Assert.assertEquals(0, myFileSystem.getDownloadTasksQueued());
    }

    @Test
    public void testAsyncDownloadFailure() throws Exception {
        Future<Path> future = failureFileSystem.asyncDownloadFile(getDummyPath(), 2);
        try {
            future.get();
        } catch (Exception e) {
            // Verify the failure metric for upload is incremented.
            Assert.assertEquals(1, (int) backupMetrics.getInvalidDownloads().count());
        }
    }

    class FailureFileSystem extends NullBackupFileSystem {

        @Inject
        public FailureFileSystem(
                IConfiguration configuration,
                BackupMetrics backupMetrics,
                BackupNotificationMgr backupNotificationMgr,
                Provider<AbstractBackupPath> pathProvider) {
            super(configuration, backupMetrics, backupNotificationMgr, pathProvider);
        }

        @Override
        protected void downloadFileImpl(AbstractBackupPath path, String suffix)
                throws BackupRestoreException {
            throw new BackupRestoreException(
                    "User injected failure file system error for testing download. Remote path: "
                            + path.getRemotePath());
        }

        @Override
        protected long uploadFileImpl(AbstractBackupPath path, Instant target)
                throws BackupRestoreException {
            throw new BackupRestoreException(
                    "User injected failure file system error for testing upload. Local path: "
                            + path.getBackupFile().getAbsolutePath());
        }
    }

    class MyFileSystem extends NullBackupFileSystem {

        private final Random random = new Random();

        @Inject
        public MyFileSystem(
                IConfiguration configuration,
                BackupMetrics backupMetrics,
                BackupNotificationMgr backupNotificationMgr,
                Provider<AbstractBackupPath> pathProvider) {
            super(configuration, backupMetrics, backupNotificationMgr, pathProvider);
        }

        @Override
        protected void downloadFileImpl(AbstractBackupPath path, String suffix)
                throws BackupRestoreException {
            try {
                Thread.sleep(random.nextInt(20));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected long uploadFileImpl(AbstractBackupPath path, Instant target)
                throws BackupRestoreException {
            try {
                Thread.sleep(random.nextInt(20));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return 0;
        }
    }
}
