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

import com.google.inject.Inject;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.TaskTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 3/9/19. */
public class BackupV2Service implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration configuration;
    private final IBackupRestoreConfig backupRestoreConfig;
    private final SnapshotMetaTask snapshotMetaTask;
    private static final Logger logger = LoggerFactory.getLogger(BackupV2Service.class);

    @Inject
    public BackupV2Service(
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig,
            PriamScheduler scheduler,
            SnapshotMetaTask snapshotMetaService) {
        this.configuration = configuration;
        this.backupRestoreConfig = backupRestoreConfig;
        this.scheduler = scheduler;
        this.snapshotMetaTask = snapshotMetaService;
    }

    @Override
    public void scheduleService() throws Exception {
        TaskTimer snapshotMetaServiceTimer = SnapshotMetaTask.getTimer(backupRestoreConfig);
        if (snapshotMetaServiceTimer != null) {
            scheduler.addTask(
                    SnapshotMetaTask.JOBNAME, SnapshotMetaTask.class, snapshotMetaServiceTimer);
            logger.info(
                    "Added {} Task with schedule: [{}]",
                    SnapshotMetaTask.JOBNAME,
                    snapshotMetaServiceTimer.getCronExpression());

            // Try to upload previous snapshots, if any which might have been interrupted by Priam
            // restart.
            snapshotMetaTask.uploadFiles();

            // Schedule the TTL service
            TaskTimer backupTTLTimer = BackupTTLTask.getTimer(backupRestoreConfig);
            if (backupTTLTimer != null) {
                scheduler.addTask(BackupTTLTask.JOBNAME, BackupTTLTask.class, backupTTLTimer);
                logger.info(
                        "Added {} Task with schedule: [{}]",
                        BackupTTLTask.JOBNAME,
                        backupTTLTimer.getCronExpression());
            }

            // Schedule the backup verification service
            TaskTimer backupVerificationTimer =
                    BackupVerificationTask.getTimer(backupRestoreConfig);
            if (backupVerificationTimer != null) {
                scheduler.addTask(
                        BackupVerificationTask.JOBNAME,
                        BackupVerificationTask.class,
                        backupVerificationTimer);
                logger.info(
                        "Added {} Task with schedule: [{}]",
                        BackupVerificationTask.JOBNAME,
                        backupVerificationTimer.getCronExpression());
            }

            // Start the Incremental backup schedule if enabled
            if (configuration.isIncrementalBackupEnabled()
                    && backupRestoreConfig.enableV2Backups()) {
                // Delete the old task, if scheduled. This is required, as we stop taking backup
                // 1.0, we still want to take incremental backups
                // Once backup 1.0 is gone, we should not check for enableV2Backups..
                scheduler.deleteTask(IncrementalBackup.JOBNAME);
                scheduler.addTask(
                        IncrementalBackup.JOBNAME,
                        IncrementalBackup.class,
                        IncrementalBackup.getTimer());
                logger.info("Added incremental backup job");
            }
        }
    }
}
