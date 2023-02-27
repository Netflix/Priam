/*
 * Copyright 2013 Netflix, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backupv2.ForgottenFilesManager;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.CassandraOperations;
import com.netflix.priam.health.CassandraMonitor;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.ThreadSleeper;
import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for running daily snapshots */
@Singleton
public class SnapshotBackup extends AbstractBackup {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    public static final String JOBNAME = "SnapshotBackup";
    private final MetaData metaData;
    private final ThreadSleeper sleeper = new ThreadSleeper();
    private static final long WAIT_TIME_MS = 60 * 1000 * 10;
    private final InstanceIdentity instanceIdentity;
    private final IBackupStatusMgr snapshotStatusMgr;
    private final BackupRestoreUtil backupRestoreUtil;
    private final ForgottenFilesManager forgottenFilesManager;
    private String snapshotName = null;
    private Instant snapshotInstant = DateUtil.getInstant();
    private List<AbstractBackupPath> abstractBackupPaths = null;
    private final CassandraOperations cassandraOperations;
    private final BackupHelper backupHelper;
    private static final Lock lock = new ReentrantLock();

    @Inject
    public SnapshotBackup(
            IConfiguration config,
            BackupHelper backupHelper,
            MetaData metaData,
            IBackupStatusMgr snapshotStatusMgr,
            InstanceIdentity instanceIdentity,
            CassandraOperations cassandraOperations,
            ForgottenFilesManager forgottenFilesManager) {
        super(config);
        this.backupHelper = backupHelper;
        this.metaData = metaData;
        this.snapshotStatusMgr = snapshotStatusMgr;
        this.instanceIdentity = instanceIdentity;
        this.cassandraOperations = cassandraOperations;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getSnapshotIncludeCFList(), config.getSnapshotExcludeCFList());
        this.forgottenFilesManager = forgottenFilesManager;
    }

    @Override
    public void execute() throws Exception {
        // If Cassandra is started then only start Snapshot Backup
        while (!CassandraMonitor.hasCassadraStarted()) {
            logger.debug(
                    "Cassandra has not yet started, hence Snapshot Backup will start after ["
                            + WAIT_TIME_MS / 1000
                            + "] secs ...");
            sleeper.sleep(WAIT_TIME_MS);
        }

        // Do not allow more than one snapshot to run at the same time. This is possible as this
        // happens on CRON.
        if (!lock.tryLock()) {
            logger.warn("Snapshot Operation is already running! Try again later.");
            throw new Exception("Snapshot Operation already running");
        }

        try {
            // Clean up all the backup directories, if any.
            cleanOldBackups(config);
            executeSnapshot();
        } finally {
            lock.unlock();
        }
    }

    private void executeSnapshot() throws Exception {
        Date startTime = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime();
        snapshotName = DateUtil.formatyyyyMMddHHmm(startTime);
        snapshotInstant = DateUtil.getInstant();
        String token = instanceIdentity.getInstance().getToken();

        // Save start snapshot status
        BackupMetadata backupMetadata =
                new BackupMetadata(BackupVersion.SNAPSHOT_BACKUP, token, startTime);
        snapshotStatusMgr.start(backupMetadata);

        try {
            logger.info("Starting snapshot {}", snapshotName);
            cassandraOperations.takeSnapshot(snapshotName);
            backupMetadata.setCassandraSnapshotSuccess(true);

            // Collect all snapshot dir's under keyspace dir's
            abstractBackupPaths = Lists.newArrayList();
            // Try to upload all the files as part of snapshot. If there is any error, there will be
            // an exception and snapshot will be considered as failure.
            initiateBackup(SNAPSHOT_FOLDER, backupRestoreUtil);

            // All the files are uploaded successfully as part of snapshot.
            // pre condition notify of meta.json upload
            File tmpMetaFile = metaData.createTmpMetaFile();
            // Note: no need to remove this temp as it is done within createTmpMetaFile()
            AbstractBackupPath metaJsonAbp = metaData.decorateMetaJson(tmpMetaFile, snapshotName);

            // Upload meta file
            AbstractBackupPath metaJson = metaData.set(abstractBackupPaths, snapshotName);

            logger.info("Snapshot upload complete for {}", snapshotName);
            backupMetadata.setSnapshotLocation(
                    config.getBackupPrefix() + File.separator + metaJson.getRemotePath());
            snapshotStatusMgr.finish(backupMetadata);
        } catch (Exception e) {
            logger.error(
                    "Exception occurred while taking snapshot: {}. Exception: {}",
                    snapshotName,
                    e.getLocalizedMessage());
            e.printStackTrace();
            snapshotStatusMgr.failed(backupMetadata);
            throw e;
        } finally {
            try {
                cassandraOperations.clearSnapshot(snapshotName);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private File getValidSnapshot(File snpDir, String snapshotName) {
        for (File snapshotDir : snpDir.listFiles())
            if (snapshotDir.getName().matches(snapshotName)) return snapshotDir;
        return null;
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static boolean isBackupEnabled(IConfiguration config) throws Exception {
        return (getTimer(config) != null);
    }

    public static TaskTimer getTimer(IConfiguration config) throws Exception {
        TaskTimer timer = CronTimer.getCronTimer(JOBNAME, config.getBackupCronExpression());
        if (timer == null) {
            // Clean up all the backup directories, if any.
            cleanOldBackups(config);
        }
        return timer;
    }

    private static void cleanOldBackups(IConfiguration configuration) throws Exception {
        Set<Path> backupPaths = AbstractBackup.getBackupDirectories(configuration, SNAPSHOT_FOLDER);
        for (Path backupDirPath : backupPaths)
            try (DirectoryStream<Path> directoryStream =
                    Files.newDirectoryStream(backupDirPath, path -> Files.isDirectory(path))) {
                for (Path backupDir : directoryStream) {
                    if (isValidBackupDir(backupDir)) {
                        FileUtils.deleteDirectory(backupDir.toFile());
                    }
                }
            }
    }

    @Override
    protected void processColumnFamily(File backupDir) throws Exception {
        File snapshotDir = getValidSnapshot(backupDir, snapshotName);

        if (snapshotDir == null) {
            logger.warn("{} folder does not contain {} snapshots", backupDir, snapshotName);
            return;
        }

        forgottenFilesManager.findAndMoveForgottenFiles(snapshotInstant, snapshotDir);
        // Add files to this dir

        ImmutableList<ListenableFuture<AbstractBackupPath>> futures =
                backupHelper.uploadAndDeleteAllFiles(
                        snapshotDir, BackupFileType.SNAP, config.enableAsyncSnapshot());
        for (Future<AbstractBackupPath> future : futures) {
            abstractBackupPaths.add(future.get());
        }
    }

    private static boolean isValidBackupDir(Path backupDir) {
        String backupDirName = backupDir.toFile().getName();
        // Check if it of format yyyyMMddHHmm
        return (DateUtil.getDate(backupDirName) != null);
    }
}
