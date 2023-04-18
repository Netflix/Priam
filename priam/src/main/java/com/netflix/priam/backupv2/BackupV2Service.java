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

import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.identity.token.ITokenRetriever;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.tuner.CassandraTunerService;
import javax.inject.Inject;

/**
 * Encapsulate the backup service 2.0 - Execute all the tasks required to run backup service.
 * Created by aagrawal on 3/9/19.
 */
public class BackupV2Service implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration configuration;
    private final IBackupRestoreConfig backupRestoreConfig;
    private final SnapshotMetaTask snapshotMetaTask;
    private final CassandraTunerService cassandraTunerService;
    private final ITokenRetriever tokenRetriever;

    @Inject
    public BackupV2Service(
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig,
            PriamScheduler scheduler,
            SnapshotMetaTask snapshotMetaService,
            CassandraTunerService cassandraTunerService,
            ITokenRetriever tokenRetriever) {
        this.configuration = configuration;
        this.backupRestoreConfig = backupRestoreConfig;
        this.scheduler = scheduler;
        this.snapshotMetaTask = snapshotMetaService;
        this.cassandraTunerService = cassandraTunerService;
        this.tokenRetriever = tokenRetriever;
    }

    @Override
    public void scheduleService() throws Exception {
        TaskTimer snapshotMetaTimer = SnapshotMetaTask.getTimer(backupRestoreConfig);
        if (snapshotMetaTimer == null) {
            SnapshotMetaTask.cleanOldBackups(configuration);
        }
        scheduleTask(scheduler, SnapshotMetaTask.class, snapshotMetaTimer);

        if (snapshotMetaTimer != null) {
            // Try to upload previous snapshots, if any which might have been interrupted by Priam
            // restart.
            snapshotMetaTask.uploadFiles();

            // Schedule the backup verification service
            scheduleTask(
                    scheduler,
                    BackupVerificationTask.class,
                    BackupVerificationTask.getTimer(backupRestoreConfig));
        } else {
            scheduler.deleteTask(BackupVerificationTask.JOBNAME);
        }

        // Schedule the TTL service
        TaskTimer timer =
                BackupTTLTask.getTimer(backupRestoreConfig, tokenRetriever.getRingPosition());
        scheduleTask(scheduler, BackupTTLTask.class, timer);

        // Start the Incremental backup schedule if enabled
        scheduleTask(
                scheduler,
                IncrementalBackup.class,
                IncrementalBackup.getTimer(configuration, backupRestoreConfig));
    }

    @Override
    public void updateServicePre() throws Exception {
        // Update the cassandra to enable/disable new incremental files.
        cassandraTunerService.onChangeUpdateService();
    }

    @Override
    public void updateServicePost() throws Exception {}
}
