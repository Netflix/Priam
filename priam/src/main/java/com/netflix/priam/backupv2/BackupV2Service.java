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
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.tuner.TuneCassandra;
import com.netflix.priam.utils.RetryableCallable;
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
        TaskTimer snapshotMetaTimer = SnapshotMetaTask.getTimer(configuration, backupRestoreConfig);
        if (snapshotMetaTimer != null) {
            scheduleTask(scheduler, SnapshotMetaTask.class, snapshotMetaTimer);

            // Try to upload previous snapshots, if any which might have been interrupted by Priam
            // restart.
            snapshotMetaTask.uploadFiles();

            // Schedule the TTL service
            scheduleTask(
                    scheduler, BackupTTLTask.class, BackupTTLTask.getTimer(backupRestoreConfig));

            // Schedule the backup verification service
            scheduleTask(
                    scheduler,
                    BackupVerificationTask.class,
                    BackupVerificationTask.getTimer(backupRestoreConfig));
        }

        // Start the Incremental backup schedule if enabled
        scheduleTask(
                scheduler,
                IncrementalBackup.class,
                IncrementalBackup.getTimer(configuration, backupRestoreConfig));
    }

    public void updateService() throws Exception {
        // Update the cassandra to stop writing new incremental files, if any.
        new RetryableCallable<Void>(6, 10000) {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                nodetool.setIncrementalBackupsEnabled(
                        IncrementalBackup.isEnabled(configuration, backupRestoreConfig));
                return null;
            }
        }.call();
        // Re-schedule services.
        scheduleService();
        // Re-write the cassandra.yaml so if cassandra restarts it is a NO-OP
        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);
    }
}
