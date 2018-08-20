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
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.ThreadSleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Task for running daily snapshots
 */
@Singleton
public class SnapshotBackup extends AbstractBackup {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    public static final String JOBNAME = "SnapshotBackup";
    private final MetaData metaData;
    private final List<String> snapshotRemotePaths = new ArrayList<String>();
    private static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();
    private final ThreadSleeper sleeper = new ThreadSleeper();
    private static final long WAIT_TIME_MS = 60 * 1000 * 10;
    private InstanceIdentity instanceIdentity;
    private IBackupStatusMgr snapshotStatusMgr;
    private BackupRestoreUtil backupRestoreUtil;
    private String snapshotName = null;
    private List<AbstractBackupPath> abstractBackupPaths = null;
    private CassandraOperations cassandraOperations;

    @Inject
    public SnapshotBackup(IConfiguration config, Provider<AbstractBackupPath> pathFactory,
                          MetaData metaData, IFileSystemContext backupFileSystemCtx
            , IBackupStatusMgr snapshotStatusMgr
            , InstanceIdentity instanceIdentity, CassandraOperations cassandraOperations) {
        super(config, backupFileSystemCtx, pathFactory);
        this.metaData = metaData;
        this.snapshotStatusMgr = snapshotStatusMgr;
        this.instanceIdentity = instanceIdentity;
        this.cassandraOperations = cassandraOperations;
        backupRestoreUtil = new BackupRestoreUtil(config.getSnapshotKeyspaceFilters(), config.getSnapshotCFFilter());
    }

    @Override
    public void execute() throws Exception {
        //If Cassandra is started then only start Snapshot Backup
        while (!CassandraMonitor.hasCassadraStarted()) {
            logger.debug("Cassandra has not yet started, hence Snapshot Backup will start after [" + WAIT_TIME_MS / 1000 + "] secs ...");
            sleeper.sleep(WAIT_TIME_MS);
        }

        Date startTime = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime();
        snapshotName = pathFactory.get().formatDate(startTime);
        String token = instanceIdentity.getInstance().getToken();

        // Save start snapshot status
        BackupMetadata backupMetadata = new BackupMetadata(token, startTime);
        snapshotStatusMgr.start(backupMetadata);

        try {
            logger.info("Starting snapshot {}", snapshotName);
            //Clearing remotePath List
            snapshotRemotePaths.clear();
            cassandraOperations.takeSnapshot(snapshotName);

            // Collect all snapshot dir's under keyspace dir's
            abstractBackupPaths = Lists.newArrayList();
            // Try to upload all the files as part of snapshot. If there is any error, there will be an exception and snapshot will be considered as failure.
            initiateBackup(SNAPSHOT_FOLDER, backupRestoreUtil);

            // All the files are uploaded successfully as part of snapshot.
            //pre condition notifiy of meta.json upload
            File tmpMetaFile = metaData.createTmpMetaFile(); //Note: no need to remove this temp as it is done within createTmpMetaFile()
            AbstractBackupPath metaJsonAbp = metaData.decorateMetaJson(tmpMetaFile, snapshotName);

            // Upload meta file
            AbstractBackupPath metaJson = metaData.set(abstractBackupPaths, snapshotName);

            logger.info("Snapshot upload complete for {}", snapshotName);
            backupMetadata.setSnapshotLocation(config.getBackupPrefix() + File.separator + metaJson.getRemotePath());
            snapshotStatusMgr.finish(backupMetadata);

            if (snapshotRemotePaths.size() > 0) {
                notifyObservers();
            }

        } catch (Exception e) {
            logger.error("Exception occurred while taking snapshot: {}. Exception: {}", snapshotName, e.getLocalizedMessage());
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
            if (snapshotDir.getName().matches(snapshotName))
                return snapshotDir;
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
                    logger.info("Skipping {} as it is disabled via backup hour: {}", JOBNAME, config.getBackupHour());
                else {
                    cronTimer = new CronTimer(JOBNAME, config.getBackupHour(), 1, 0);
                    logger.info("Starting snapshot backup with backup hour: {}", config.getBackupHour());
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
            } else
                logger.info("Observer is Null, hence can not notify ...");
        }
    }

    @Override
    protected void processColumnFamily(String keyspace, String columnFamily, File backupDir) throws Exception {

        File snapshotDir = getValidSnapshot(backupDir, snapshotName);
        // Add files to this dir
        if (null != snapshotDir)
            abstractBackupPaths.addAll(upload(snapshotDir, BackupFileType.SNAP));
        else
            logger.warn("{} folder does not contain {} snapshots", backupDir, snapshotName);
    }

    @Override
    protected void addToRemotePath(String remotePath) {
        snapshotRemotePaths.add(remotePath);
    }
}
