/**
 * Copyright 2013 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.notification.BackupEvent;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.EventObserver;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Task for running daily snapshots
 */
@Singleton
public class SnapshotBackup extends AbstractBackup{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    public static final String JOBNAME = "SnapshotBackup";
    private final MetaData metaData;
    private final List<String> snapshotRemotePaths = new ArrayList<String>();
    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();
    private final ThreadSleeper sleeper = new ThreadSleeper();
    private static final long WAIT_TIME_MS = 60 * 1000 * 10;
    private final CommitLogBackup clBackup;
    private InstanceIdentity instanceIdentity;
    private IBackupStatusMgr snapshotStatusMgr;
    private BackupRestoreUtil backupRestoreUtil;

    @Inject
    public SnapshotBackup(IConfiguration config, Provider<AbstractBackupPath> pathFactory,
                          MetaData metaData, CommitLogBackup clBackup, @Named("backup") IFileSystemContext backupFileSystemCtx
            , IBackupStatusMgr snapshotStatusMgr
            , BackupNotificationMgr backupNotificationMgr, InstanceIdentity instanceIdentity) {
        super(config, backupFileSystemCtx, pathFactory, backupNotificationMgr);
        this.metaData = metaData;
        this.clBackup = clBackup;
        this.snapshotStatusMgr = snapshotStatusMgr;
        this.instanceIdentity = instanceIdentity;
        backupRestoreUtil = new BackupRestoreUtil(config.getSnapshotKeyspaceFilters(), config.getSnapshotCFFilter());
    }

    @Override
    public void execute() throws Exception {
        //If Cassandra is started then only start Snapshot Backup
        while (!CassandraMonitor.isCassadraStarted()) {
            logger.debug("Cassandra is not yet started, hence Snapshot Backup will start after [" + WAIT_TIME_MS / 1000 + "] secs ...");
            sleeper.sleep(WAIT_TIME_MS);
        }

        Date startTime = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime();
        String snapshotName = pathFactory.get().formatDate(startTime);
        String token = instanceIdentity.getInstance().getToken();

        // Save start snapshot status
        BackupMetadata backupMetadata = new BackupMetadata(token, startTime);
        snapshotStatusMgr.start(backupMetadata);

        try {
            logger.info("Starting snapshot " + snapshotName);
            //Clearing remotePath List
            snapshotRemotePaths.clear();
            takeSnapshot(snapshotName);

            // Collect all snapshot dir's under keyspace dir's
            List<AbstractBackupPath> bps = Lists.newArrayList();
            File dataDir = new File(config.getDataFileLocation());
            for (File keyspaceDir : dataDir.listFiles()) {
                if (keyspaceDir.isFile())
                    continue;

                if (backupRestoreUtil.isFiltered(BackupRestoreUtil.DIRECTORYTYPE.KEYSPACE, keyspaceDir.getName())) { //keyspace filtered?
                    logger.info(keyspaceDir.getName() + " is part of keyspace filter, will not be backed up.");
                    continue;
                }

                logger.debug("Entering {} keyspace..", keyspaceDir.getName());
                for (File columnFamilyDir : keyspaceDir.listFiles()) {
                    if (backupRestoreUtil.isFiltered(BackupRestoreUtil.DIRECTORYTYPE.CF, keyspaceDir.getName(), columnFamilyDir.getName())) { //CF filtered?
                        logger.info("keyspace: " + keyspaceDir.getName()
                                + ", CF: " + columnFamilyDir.getName() + " is part of CF filter list, will not be backed up.");
                        continue;
                    }

                    logger.debug("Entering {} columnFamily..", columnFamilyDir.getName());
                    File snpDir = new File(columnFamilyDir, "snapshots");
                    if (!isValidBackupDir(keyspaceDir, columnFamilyDir, snpDir)) {
                        continue;
                    }

                    File snapshotDir = getValidSnapshot(columnFamilyDir, snpDir, snapshotName);
                    // Add files to this dir
                    if (null != snapshotDir)
                        bps.addAll(upload(snapshotDir, BackupFileType.SNAP));
                    else
                        logger.warn("{} folder does not contain {} snapshots", snpDir, snapshotName);
                }

            }

            //pre condition notifiy of meta.json upload
            File tmpMetaFile = metaData.createTmpMetaFile(); //Note: no need to remove this temp as it is done within createTmpMetaFile()
            AbstractBackupPath metaJsonAbp = metaData.decorateMetaJson(tmpMetaFile, snapshotName);
            metaJsonAbp.setCompressedFileSize(0);
            notifyEventStart(new BackupEvent(metaJsonAbp));

            // Upload meta file
            AbstractBackupPath metaJson = metaData.set(bps, snapshotName);

            logger.info("Snapshot upload complete for " + snapshotName);
            notifyEventSuccess(new BackupEvent(metaJsonAbp));
            backupMetadata.setSnapshotLocation(config.getBackupPrefix() + File.separator + metaJson.getRemotePath());
            snapshotStatusMgr.finish(backupMetadata);

            if (snapshotRemotePaths.size() > 0) {
                notifyObservers();
            }

        } catch (Exception e)
        {
            logger.error("Exception occured while taking snapshot: {}. Exception: {}", snapshotName, e.getLocalizedMessage());
            snapshotStatusMgr.failed(backupMetadata);
            throw e;
        } finally{
            try {
                clearSnapshot(snapshotName);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private File getValidSnapshot(File columnFamilyDir, File snpDir, String snapshotName) {
        for (File snapshotDir : snpDir.listFiles())
            if (snapshotDir.getName().matches(snapshotName))
                return snapshotDir;
        return null;
    }

    private void takeSnapshot(final String snapshotName) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.takeSnapshot(snapshotName, null);
                //nodetool.takeSnapshot(snapshotName, null, new String[0]);
                return null;
            }
        }.call();
    }

    private void clearSnapshot(final String snapshotTag) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.clearSnapshot(snapshotTag);
                return null;
            }
        }.call();
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config) throws Exception{
        CronTimer cronTimer = null;
        switch (config.getBackupSchedulerType())
        {
            case HOUR:
                int hour = config.getBackupHour();
                if (hour >= 0) {
                    cronTimer = new CronTimer(JOBNAME, hour, 1, 0);
                    logger.info("Starting snapshot backup with backup hour: " + hour);
                }else
                    logger.info("Skipping snapshot backup as backup hour is less than 0: " + hour);
                break;
            case CRON:
                String cronExpression = config.getBackupCronExpression();

                if(!StringUtils.isEmpty(cronExpression) && cronExpression.equalsIgnoreCase("-1")){
                    logger.info("Skipping snapshot backup as backup cron is set to NA");
                }else
                {
                    if(StringUtils.isEmpty(cronExpression) || !CronExpression.isValidExpression(cronExpression))
                        throw new Exception("Invalid CRON expression: " + cronExpression +
                                ". Please use -1 if you wish to disable backup else fix the CRON expression and try again!");

                    cronTimer = new CronTimer(JOBNAME, config.getBackupCronExpression());
                    logger.info(String.format("Starting snapshot backup with CRON expression %s", cronTimer.getCronExpression()));
                }
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

    public void notifyObservers() {
        for (IMessageObserver observer : observers) {
            if (observer != null) {
                logger.debug("Updating snapshot observers now ...");
                observer.update(BACKUP_MESSAGE_TYPE.SNAPSHOT, snapshotRemotePaths);
            } else
                logger.info("Observer is Null, hence can not notify ...");
        }
    }

    @Override
    protected void addToRemotePath(String remotePath) {
        snapshotRemotePaths.add(remotePath);
    }
}
