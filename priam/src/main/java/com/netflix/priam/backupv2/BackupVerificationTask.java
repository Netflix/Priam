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

import com.netflix.priam.backup.*;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.DateUtil.DateRange;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 1/28/19. */
@Singleton
public class BackupVerificationTask extends Task {
    private static final Logger logger = LoggerFactory.getLogger(BackupVerificationTask.class);
    public static final String JOBNAME = "BackupVerificationService";

    private IBackupRestoreConfig backupRestoreConfig;
    private BackupVerification backupVerification;
    private BackupMetrics backupMetrics;
    private InstanceState instanceState;
    private BackupNotificationMgr backupNotificationMgr;

    @Inject
    public BackupVerificationTask(
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig,
            BackupVerification backupVerification,
            BackupMetrics backupMetrics,
            InstanceState instanceState,
            BackupNotificationMgr backupNotificationMgr) {
        super(configuration);
        this.backupRestoreConfig = backupRestoreConfig;
        this.backupVerification = backupVerification;
        this.backupMetrics = backupMetrics;
        this.instanceState = instanceState;
        this.backupNotificationMgr = backupNotificationMgr;
    }

    @Override
    public void execute() throws Exception {
        // Ensure that backup version 2.0 is actually enabled.
        if (backupRestoreConfig.getSnapshotMetaServiceCronExpression().equals("-1")) {
            logger.info("Skipping backup verification. V2 backups are not enabled.");
            return;
        }

        if (instanceState.getRestoreStatus() != null
                && instanceState.getRestoreStatus().getStatus() != null
                && instanceState.getRestoreStatus().getStatus() == Status.STARTED) {
            logger.info("Skipping backup verification. Priam is in restore mode.");
            return;
        }

        // Validate the backup done in last x hours.
        Instant now = DateUtil.getInstant();
        Instant slo =
                now.minus(backupRestoreConfig.getBackupVerificationSLOInHours(), ChronoUnit.HOURS);
        DateRange dateRange = new DateRange(slo, now);
        List<BackupMetadata> verifiedBackups =
                backupVerification.verifyBackupsInRange(
                        BackupVersion.SNAPSHOT_META_SERVICE, dateRange);

        verifiedBackups
                .stream()
                .filter(result -> result.getLastValidated().toInstant().isAfter(now))
                .forEach(
                        result -> {
                            logger.info(
                                    "Sending {} message for backup: {}",
                                    AbstractBackupPath.BackupFileType.SNAPSHOT_VERIFIED,
                                    result.getSnapshotLocation());
                            backupNotificationMgr.notify(
                                    result.getSnapshotLocation(), result.getStart().toInstant());
                        });

        if (verifiedBackups.isEmpty()) {
            logger.error(
                    "Not able to find any snapshot which is valid in our SLO window: {} hours",
                    backupRestoreConfig.getBackupVerificationSLOInHours());
            backupMetrics.incrementBackupVerificationFailure();
        }
    }

    /**
     * Interval between trying to verify data manifest file on Remote file system.
     *
     * @param backupRestoreConfig {@link IBackupRestoreConfig#getBackupVerificationCronExpression()}
     *     to get configuration details from priam. Use "-1" to disable the service.
     * @return the timer to be used for snapshot verification service.
     * @throws Exception if the configuration is not set correctly or are not valid. This is to
     *     ensure we fail-fast.
     */
    public static TaskTimer getTimer(IBackupRestoreConfig backupRestoreConfig) throws Exception {
        String cronExpression = backupRestoreConfig.getBackupVerificationCronExpression();
        return CronTimer.getCronTimer(JOBNAME, cronExpression);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }
}
