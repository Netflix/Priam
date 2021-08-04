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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backupv2.SnapshotMetaTask;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Incremental/SSTable backup
 */
@Singleton
public class IncrementalBackup extends AbstractBackup {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    public static final String JOBNAME = "IncrementalBackup";
    private final BackupRestoreUtil backupRestoreUtil;
    private final IBackupRestoreConfig backupRestoreConfig;

    @Inject
    public IncrementalBackup(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            Provider<AbstractBackupPath> pathFactory,
            IFileSystemContext backupFileSystemCtx) {
        super(config, backupFileSystemCtx, pathFactory);
        // a means to upload audit trail (via meta_cf_yyyymmddhhmm.json) of files successfully
        // uploaded)
        this.backupRestoreConfig = backupRestoreConfig;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getIncrementalIncludeCFList(), config.getIncrementalExcludeCFList());
    }

    @Override
    public void execute() throws Exception {
        // Clearing remotePath List
        initiateBackup(INCREMENTAL_BACKUP_FOLDER, backupRestoreUtil);
    }

    /** Run every 10 Sec */
    public static TaskTimer getTimer(
            IConfiguration config, IBackupRestoreConfig backupRestoreConfig) {
        if (IncrementalBackup.isEnabled(config, backupRestoreConfig))
            return new SimpleTimer(JOBNAME, 10L * 1000);
        return null;
    }

    private static void cleanOldBackups(IConfiguration configuration) throws Exception {
        Set<Path> backupPaths =
                AbstractBackup.getBackupDirectories(configuration, INCREMENTAL_BACKUP_FOLDER);
        for (Path backupDirPath : backupPaths) {
            FileUtils.cleanDirectory(backupDirPath.toFile());
        }
    }

    public static boolean isEnabled(
            IConfiguration configuration, IBackupRestoreConfig backupRestoreConfig) {
        boolean enabled = false;
        try {
            // Once backup 1.0 is gone, we should not check for enableV2Backups.
            enabled =
                    (configuration.isIncrementalBackupEnabled()
                            && (SnapshotBackup.isBackupEnabled(configuration)
                                    || (backupRestoreConfig.enableV2Backups()
                                            && SnapshotMetaTask.isBackupEnabled(
                                                    configuration, backupRestoreConfig))));
            logger.info("Incremental backups are enabled: {}", enabled);

            if (!enabled) {
                // Clean up the incremental backup folder.
                cleanOldBackups(configuration);
            }
        } catch (Exception e) {
            logger.error(
                    "Error while trying to find if incremental backup is enabled: "
                            + e.getMessage());
        }
        return enabled;
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    @Override
    protected void processColumnFamily(File backupDir) throws Exception {
        BackupFileType fileType =
                backupRestoreConfig.enableV2Backups() ? BackupFileType.SST_V2 : BackupFileType.SST;

        // upload SSTables and components
        ImmutableList<ListenableFuture<AbstractBackupPath>> futures =
                uploadAndDeleteAllFiles(backupDir, fileType, config.enableAsyncIncremental());
        Futures.whenAllComplete(futures).call(() -> null, MoreExecutors.directExecutor());

        // Next, upload secondary indexes
        fileType = BackupFileType.SECONDARY_INDEX_V2;
        for (File directory : getSecondaryIndexDirectories(backupDir)) {
            futures = uploadAndDeleteAllFiles(directory, fileType, config.enableAsyncIncremental());
            if (futures.stream().allMatch(ListenableFuture::isDone)) {
                deleteIfEmpty(directory);
            } else {
                Futures.whenAllComplete(futures)
                        .call(() -> deleteIfEmpty(directory), MoreExecutors.directExecutor());
            }
        }
    }

    private Void deleteIfEmpty(File dir) {
        if (FileUtils.sizeOfDirectory(dir) == 0) FileUtils.deleteQuietly(dir);
        return null;
    }
}
