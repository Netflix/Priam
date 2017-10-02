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
package com.netflix.priam.restore;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.MetaData;
import com.netflix.priam.backup.Status;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Main class for restoring data from backup
 */
@Singleton
public class Restore extends AbstractRestore {
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    private final ICassandraProcess cassProcess;
    @Inject
    private MetaData metaData;
    private RestoreStatus restoreStatus;

    @Inject
    public Restore(IConfiguration config, @Named("backup") IBackupFileSystem fs, Sleeper sleeper, ICassandraProcess cassProcess,
                   Provider<AbstractBackupPath> pathProvider,
                   InstanceIdentity instanceIdentity, RestoreTokenSelector tokenSelector, RestoreStatus restoreStatus) {
        super(config, fs, JOBNAME, sleeper, pathProvider, instanceIdentity, tokenSelector, cassProcess);
        this.cassProcess = cassProcess;
        this.restoreStatus = restoreStatus;
    }


    /**
     * Restore backup data for the specified time range
     */
    public void restore(Date startTime, Date endTime) throws Exception {

        restoreStatus.resetStatus();
        restoreStatus.setStartDateRange(DateUtil.convert(startTime));
        restoreStatus.setEndDateRange(DateUtil.convert(endTime));
        restoreStatus.setExecStartTime(LocalDateTime.now());
        restoreStatus.setStatus(Status.STARTED);

        // Stop cassandra if its running and restoring all keyspaces
        stopCassProcess();

        // Cleanup local data
        SystemUtils.cleanupDir(config.getDataFileLocation(), config.getRestoreKeySpaces());

        // Try and read the Meta file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        String prefix = getRestorePrefix();
        fetchSnapshotMetaFile(prefix, metas, startTime, endTime);

        if (metas.size() == 0) {
            logger.info("[cass_backup] No snapshot meta file found, Restore Failed.");
            restoreStatus.setExecEndTime(LocalDateTime.now());
            restoreStatus.setStatus(Status.FINISHED);
            return;
        }

        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Snapshot Meta file for restore " + meta.getRemotePath());
        restoreStatus.setSnapshotMetaFile(meta.getRemotePath());

        // Download snapshot which is listed in the meta file.
        List<AbstractBackupPath> snapshots = metaData.get(meta);
        download(snapshots.iterator(), BackupFileType.SNAP);

        logger.info("Downloading incrementals");
        // Download incrementals (SST) after the snapshot meta file.
        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.getTime(), endTime);
        download(incrementals, BackupFileType.SST);

        //Downloading CommitLogs
        if (config.isBackingUpCommitLogs())  //TODO: will change to isRestoringCommitLogs()
        {
            logger.info("Delete all backuped commitlog files in " + config.getBackupCommitLogLocation());
            SystemUtils.cleanupDir(config.getBackupCommitLogLocation(), null);

            logger.info("Delete all commitlog files in " + config.getCommitLogLocation());
            SystemUtils.cleanupDir(config.getCommitLogLocation(), null);

            Iterator<AbstractBackupPath> commitLogPathIterator = fs.list(prefix, meta.getTime(), endTime);
            download(commitLogPathIterator, BackupFileType.CL, config.maxCommitLogsRestore());
        }

        restoreStatus.setExecEndTime(LocalDateTime.now());
        restoreStatus.setStatus(Status.FINISHED);
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public int getActiveCount() {
        return (executor == null) ? 0 : executor.getActiveCount();
    }
}