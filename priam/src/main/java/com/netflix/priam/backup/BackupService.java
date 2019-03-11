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
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.PriamScheduler;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 3/9/19. */
public class BackupService implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private final InstanceIdentity instanceIdentity;
    private static final Logger logger = LoggerFactory.getLogger(BackupService.class);

    @Inject
    public BackupService(
            IConfiguration config,
            PriamScheduler priamScheduler,
            InstanceIdentity instanceIdentity) {
        this.config = config;
        this.scheduler = priamScheduler;
        this.instanceIdentity = instanceIdentity;
    }

    @Override
    public void scheduleService() throws Exception {
        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1) or set backup cron to "-1"
        if (SnapshotBackup.getTimer(config) != null
                && (CollectionUtils.isEmpty(config.getBackupRacs())
                        || config.getBackupRacs()
                                .contains(instanceIdentity.getInstanceInfo().getRac()))) {
            scheduler.addTask(
                    SnapshotBackup.JOBNAME, SnapshotBackup.class, SnapshotBackup.getTimer(config));

            // Start the Incremental backup schedule if enabled
            if (config.isIncrementalBackupEnabled()) {
                scheduler.addTask(
                        IncrementalBackup.JOBNAME,
                        IncrementalBackup.class,
                        IncrementalBackup.getTimer());
                logger.info("Added incremental backup job");
            }
        }

        if (config.isBackingUpCommitLogs()) {
            scheduler.addTask(
                    CommitLogBackupTask.JOBNAME,
                    CommitLogBackupTask.class,
                    CommitLogBackupTask.getTimer(config));
        }

        // Set cleanup
        scheduler.addTask(
                UpdateCleanupPolicy.JOBNAME,
                UpdateCleanupPolicy.class,
                UpdateCleanupPolicy.getTimer());
    }
}
