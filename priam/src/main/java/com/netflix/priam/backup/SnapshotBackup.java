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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.CassandraOperations;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.ThreadSleeper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for running daily snapshots */
@Singleton
public class SnapshotBackup extends AbstractBackup {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    public static final String JOBNAME = "SnapshotBackup";
    private final MetaData metaData;
    private final List<String> snapshotRemotePaths = new ArrayList<>();
    private static final List<IMessageObserver> observers = new ArrayList<>();
    private final ThreadSleeper sleeper = new ThreadSleeper();
    private static final long WAIT_TIME_MS = 60 * 1000 * 10;
    private final InstanceIdentity instanceIdentity;
    private final IBackupStatusMgr snapshotStatusMgr;
    private final BackupRestoreUtil backupRestoreUtil;
    private String snapshotName = null;
    private Instant snapshotInstant = DateUtil.getInstant();
    private List<AbstractBackupPath> abstractBackupPaths = null;
    private final CassandraOperations cassandraOperations;
    private BackupMetrics backupMetrics;
    private static final Lock lock = new ReentrantLock();

    private final String TMP_EXT = ".tmp";
    private final Pattern tmpFilePattern =
            Pattern.compile("^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$");

    @Inject
    public SnapshotBackup(
            IConfiguration config,
            Provider<AbstractBackupPath> pathFactory,
            MetaData metaData,
            IFileSystemContext backupFileSystemCtx,
            IBackupStatusMgr snapshotStatusMgr,
            InstanceIdentity instanceIdentity,
            CassandraOperations cassandraOperations,
            BackupMetrics backupMetrics) {
        super(config, backupFileSystemCtx, pathFactory);
        this.metaData = metaData;
        this.backupMetrics = backupMetrics;
        this.snapshotStatusMgr = snapshotStatusMgr;
        this.instanceIdentity = instanceIdentity;
        this.cassandraOperations = cassandraOperations;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getSnapshotIncludeCFList(), config.getSnapshotExcludeCFList());
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
            executeSnapshot();
        } finally {
            lock.unlock();
        }
    }

    private void executeSnapshot() throws Exception {
        Date startTime = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime();
        snapshotName = pathFactory.get().formatDate(startTime);
        snapshotInstant = DateUtil.getInstant();
        String token = instanceIdentity.getInstance().getToken();

        // Save start snapshot status
        BackupMetadata backupMetadata = new BackupMetadata(token, startTime);
        snapshotStatusMgr.start(backupMetadata);

        try {
            logger.info("Starting snapshot {}", snapshotName);
            // Clearing remotePath List
            snapshotRemotePaths.clear();
            cassandraOperations.takeSnapshot(snapshotName);

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

            if (snapshotRemotePaths.size() > 0) {
                notifyObservers();
            }

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
        CronTimer cronTimer = null;
        switch (config.getBackupSchedulerType()) {
            case HOUR:
                if (config.getBackupHour() < 0)
                    logger.info(
                            "Skipping {} as it is disabled via backup hour: {}",
                            JOBNAME,
                            config.getBackupHour());
                else {
                    cronTimer = new CronTimer(JOBNAME, config.getBackupHour(), 1, 0);
                    logger.info(
                            "Starting snapshot backup with backup hour: {}",
                            config.getBackupHour());
                }
                break;
            case CRON:
                cronTimer = CronTimer.getCronTimer(JOBNAME, config.getBackupCronExpression());
                break;
        }
        return cronTimer;
    }

    public static void addObserver(IMessageObserver observer) {
        observers.add(observer);
    }

    public static void removeObserver(IMessageObserver observer) {
        observers.remove(observer);
    }

    private void notifyObservers() {
        for (IMessageObserver observer : observers) {
            if (observer != null) {
                logger.debug("Updating snapshot observers now ...");
                observer.update(BACKUP_MESSAGE_TYPE.SNAPSHOT, snapshotRemotePaths);
            } else logger.info("Observer is Null, hence can not notify ...");
        }
    }

    @Override
    protected void processColumnFamily(String keyspace, String columnFamily, File backupDir)
            throws Exception {

        File snapshotDir = getValidSnapshot(backupDir, snapshotName);

        if (snapshotDir == null) {
            logger.warn("{} folder does not contain {} snapshots", backupDir, snapshotName);
            return;
        }

        findAndMoveForgottenFiles(snapshotDir);
        // Add files to this dir
        abstractBackupPaths.addAll(
                upload(snapshotDir, BackupFileType.SNAP, config.enableAsyncSnapshot(), true));
    }

    private void findAndMoveForgottenFiles(File snapshotDir) {
        try {
            Collection<File> snapshotFiles =
                    FileUtils.listFiles(snapshotDir, FileFilterUtils.fileFileFilter(), null);
            File columnfamilyDir = snapshotDir.getParentFile().getParentFile();

            // Find all the files in columnfamily folder which is :
            // 1. Not a temp file.
            // 2. Is a file. (we don't care about directories)
            // 3. Is older than snapshot time, as new files keep getting created after taking a
            // snapshot.
            IOFileFilter tmpFileFilter1 = FileFilterUtils.suffixFileFilter(TMP_EXT);
            IOFileFilter tmpFileFilter2 =
                    FileFilterUtils.asFileFilter(
                            pathname -> tmpFilePattern.matcher(pathname.getName()).matches());
            IOFileFilter tmpFileFilter = FileFilterUtils.or(tmpFileFilter1, tmpFileFilter2);
            // Here we are allowing files which were more than
            // @link{IConfiguration#getForgottenFileGracePeriodDays}. We do this to allow cassandra
            // to
            // clean up any files which were generated as part of repair/compaction and cleanup
            // thread has not already deleted.
            // Refer to https://issues.apache.org/jira/browse/CASSANDRA-6756 and
            // https://issues.apache.org/jira/browse/CASSANDRA-7066
            // for more information.
            IOFileFilter ageFilter =
                    FileFilterUtils.ageFileFilter(
                            snapshotInstant
                                    .minus(
                                            config.getForgottenFileGracePeriodDays(),
                                            ChronoUnit.DAYS)
                                    .toEpochMilli());
            IOFileFilter fileFilter =
                    FileFilterUtils.and(
                            FileFilterUtils.notFileFilter(tmpFileFilter),
                            FileFilterUtils.fileFileFilter(),
                            ageFilter);

            Collection<File> columnfamilyFiles =
                    FileUtils.listFiles(columnfamilyDir, fileFilter, null);

            // Remove the SSTable(s) which are part of snapshot from the CF file list.
            // This cannot be a simple removeAll as snapshot files have "different" file folder
            // prefix.
            for (File file : snapshotFiles) {
                // Get its parent directory file based on this file.
                File originalFile = new File(columnfamilyDir, file.getName());
                columnfamilyFiles.remove(originalFile);
            }

            // If there are no "extra" SSTables in CF data folder, we are done.
            if (columnfamilyFiles.size() == 0) return;

            logger.warn(
                    "# of forgotten files: {} found for CF: {}",
                    columnfamilyFiles.size(),
                    columnfamilyDir.getName());
            backupMetrics.incrementForgottenFiles(columnfamilyFiles.size());

            // Move the files to lost_found directory if configured.
            final Path destDir = Paths.get(columnfamilyDir.getAbsolutePath(), "lost+found");
            for (File file : columnfamilyFiles) {
                logger.warn(
                        "Forgotten file: {} found for CF: {}",
                        file.getAbsolutePath(),
                        columnfamilyDir.getName());
                if (config.isForgottenFileMoveEnabled()) {
                    try {
                        FileUtils.moveFileToDirectory(file, destDir.toFile(), true);
                    } catch (IOException e) {
                        logger.error(
                                "Exception occurred while trying to move forgottenFile: {}. Ignoring the error and continuing with remaining backup/forgotten files.",
                                file);
                        e.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {
            // Eat the exception, if there, for any reason. This should not stop the snapshot for
            // any reason.
            logger.error(
                    "Exception occurred while trying to find forgottenFile. Ignoring the error and continuing with remaining backup",
                    e);
            e.printStackTrace();
        }
    }

    @Override
    protected void addToRemotePath(String remotePath) {
        snapshotRemotePaths.add(remotePath);
    }
}
