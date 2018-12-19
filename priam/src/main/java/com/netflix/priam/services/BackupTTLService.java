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

package com.netflix.priam.services;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.backupv2.BackupValidator;
import com.netflix.priam.backupv2.ColumnfamilyResult;
import com.netflix.priam.backupv2.MetaFileReader;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.DateUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 11/26/18. */
@Singleton
public class BackupTTLService extends Task {
    private static final Logger logger = LoggerFactory.getLogger(BackupTTLService.class);
    private IBackupRestoreConfig backupRestoreConfig;
    private BackupValidator backupValidator;
    private IBackupFileSystem fileSystem;
    private Provider<AbstractBackupPath> abstractBackupPathProvider;
    public static final String JOBNAME = "BackupTTLService";
    private Map<String, Boolean> filesInMeta = new HashMap<>();
    private List<Path> filesToDelete = new ArrayList<>();
    private static final Lock lock = new ReentrantLock();
    private final int BATCH_SIZE = 1000;
    private final Instant start_of_feature = DateUtil.parseInstant("201801010000");

    @Inject
    public BackupTTLService(
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig,
            BackupValidator backupValidator,
            IFileSystemContext backupFileSystemCtx,
            Provider<AbstractBackupPath> abstractBackupPathProvider) {
        super(configuration);
        this.backupRestoreConfig = backupRestoreConfig;
        this.backupValidator = backupValidator;
        this.fileSystem = backupFileSystemCtx.getFileStrategy(configuration);
        this.abstractBackupPathProvider = abstractBackupPathProvider;
    }

    @Override
    public void execute() throws Exception {
        // Ensure that backup version 2.0 is actually enabled.
        if (!backupRestoreConfig.enableV2Backups()) {
            logger.info("Not executing the TTL Service for backups as V2 backups are not enabled.");
            return;
        }

        // Do not allow more than one backupTTLService to run at the same time. This is possible
        // as this happens on CRON.
        if (!lock.tryLock()) {
            logger.warn("{} is already running! Try again later.", JOBNAME);
            throw new Exception(JOBNAME + " already running");
        }

        try {
            filesInMeta.clear();
            filesToDelete.clear();

            Instant dateToTtl =
                    DateUtil.getInstant().minus(config.getBackupRetentionDays(), ChronoUnit.DAYS);
            logger.info(
                    "Will try to delete(TTL) files which are before this time: {}. TTL in Days: {}",
                    dateToTtl.toEpochMilli(),
                    config.getBackupRetentionDays());

            // Find the snapshot just after this date.
            List<AbstractBackupPath> metas =
                    backupValidator.findMetaFiles(
                            new DateUtil.DateRange(dateToTtl, DateUtil.getInstant()));

            if (metas.size() == 0) {
                logger.info("No meta file found and thus cannot run TTL Service");
                return;
            }

            AbstractBackupPath metaFile = metas.get(metas.size() - 1);

            // Download the meta file and save the files in memory.
            Path localFile = backupValidator.downloadMetaFile(metaFile);

            // Walk over the file system iterator and if not in map, it is eligible for delete.
            new MetaFileWalker().readMeta(localFile);
            if (logger.isDebugEnabled())
                logger.debug("Files in meta file: {}", filesInMeta.keySet().toString());

            Iterator<String> remoteFileLocations =
                    fileSystem.listFileSystem(getSSTPrefix(), null, null);

            while (remoteFileLocations.hasNext()) {
                AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
                abstractBackupPath.parseRemote(remoteFileLocations.next());
                // If lastModifiedTime is after the dateToTTL, we should get out of this loop as
                // remote file systems always give locations which are sorted.
                if (abstractBackupPath.getLastModified().isAfter(dateToTtl)) {
                    logger.info(
                            "Breaking from TTL. Got a key which is after the TTL time: {}",
                            abstractBackupPath.getRemotePath());
                    break;
                }

                if (!filesInMeta.containsKey(abstractBackupPath.getRemotePath())) {
                    deleteFile(abstractBackupPath, false);
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug(
                                "Not deleting this key as it is referenced in backups: {}",
                                abstractBackupPath.getRemotePath());
                }
            }

            // Delete the  old META files. We are giving start date which is so back in past to get
            // all the META files.
            // This feature did not exist in Jan 2018.
            metas =
                    backupValidator.findMetaFiles(
                            new DateUtil.DateRange(
                                    start_of_feature, dateToTtl.minus(1, ChronoUnit.MINUTES)));

            for (AbstractBackupPath meta : metas) {
                deleteFile(meta, false);
            }

            // Delete remaining files.
            deleteFile(null, true);

            logger.info("Finished processing files for TTL service");
        } finally {
            lock.unlock();
        }
    }

    private void deleteFile(AbstractBackupPath path, boolean forceClear)
            throws BackupRestoreException {
        if (path != null) filesToDelete.add(Paths.get(path.getRemotePath()));

        if (forceClear || filesToDelete.size() >= BATCH_SIZE) {
            fileSystem.deleteRemoteFiles(filesToDelete);
            filesToDelete.clear();
        }
    }

    private String getSSTPrefix() {
        Path location = fileSystem.getPrefix();
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        return abstractBackupPath
                .remoteV2Prefix(location, AbstractBackupPath.BackupFileType.SST_V2)
                .toString();
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    /**
     * Interval between trying to TTL data on Remote file system.
     *
     * @param backupRestoreConfig {@link
     *     IBackupRestoreConfig#getSnapshotMetaServiceCronExpression()} to get configuration details
     *     from priam. Use "-1" to disable the service.
     * @return the timer to be used for snapshot meta service.
     * @throws Exception if the configuration is not set correctly or are not valid. This is to
     *     ensure we fail-fast.
     */
    public static TaskTimer getTimer(IBackupRestoreConfig backupRestoreConfig) throws Exception {
        String cronExpression = backupRestoreConfig.getBackupTTLCronExpression();
        return CronTimer.getCronTimer(JOBNAME, cronExpression);
    }

    private class MetaFileWalker extends MetaFileReader {
        @Override
        public void process(ColumnfamilyResult columnfamilyResult) {
            columnfamilyResult
                    .getSstables()
                    .forEach(
                            ssTableResult ->
                                    ssTableResult
                                            .getSstableComponents()
                                            .forEach(
                                                    fileUploadResult ->
                                                            filesInMeta.put(
                                                                    fileUploadResult
                                                                            .getBackupPath(),
                                                                    null)));
        }
    }
}
