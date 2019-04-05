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

package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.netflix.priam.aws.UpdateCleanupPolicy;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.tuner.TuneCassandra;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 3/9/19. */
public class BackupService implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private final IBackupRestoreConfig backupRestoreConfig;
    private final InstanceIdentity instanceIdentity;
    private static final Logger logger = LoggerFactory.getLogger(BackupService.class);

    @Inject
    public BackupService(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            PriamScheduler priamScheduler,
            InstanceIdentity instanceIdentity) {
        this.config = config;
        this.backupRestoreConfig = backupRestoreConfig;
        this.scheduler = priamScheduler;
        this.instanceIdentity = instanceIdentity;
    }

    @Override
    public void scheduleService() throws Exception {
        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1) or set backup cron to "-1"
        TaskTimer snapshotTimer = SnapshotBackup.getTimer(config);
        scheduleTask(scheduler, SnapshotBackup.class, snapshotTimer);

        if (snapshotTimer != null) {
            // Schedule commit log task
            scheduleTask(
                    scheduler, CommitLogBackupTask.class, CommitLogBackupTask.getTimer(config));
        }

        // Start the Incremental backup schedule if enabled
        scheduleTask(
                scheduler,
                IncrementalBackup.class,
                IncrementalBackup.getTimer(config, backupRestoreConfig));

        // Set cleanup
        scheduleTask(scheduler, UpdateCleanupPolicy.class, UpdateCleanupPolicy.getTimer());
    }

    public void updateService() throws Exception {
        // Update the cassandra to stop writing new incremental files, if any.
        new RetryableCallable<Void>(6, 10000) {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.setIncrementalBackupsEnabled(
                        IncrementalBackup.isEnabled(config, backupRestoreConfig));
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
