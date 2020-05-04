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
import com.google.inject.Singleton;
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
import java.time.temporal.ChronoUnit;
import java.util.List;
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
        if (backupRestoreConfig.getSnapshotMetaServiceCronExpression().equalsIgnoreCase("-1")) {
            logger.info(
                    "Not executing the Verification Service for backups as V2 backups are not enabled.");
            return;
        }

        if (instanceState.getRestoreStatus() != null
                && instanceState.getRestoreStatus().getStatus() != null
                && instanceState.getRestoreStatus().getStatus() == Status.STARTED) {
            logger.info(
                    "Not executing the Verification Service for backups as Priam is in restore mode.");
            return;
        }

        // Validate the backup done in last x hours.
        DateRange dateRange =
                new DateRange(
                        DateUtil.getInstant()
                                .minus(
                                        backupRestoreConfig.getBackupVerificationSLOInHours(),
                                        ChronoUnit.HOURS),
                        DateUtil.getInstant());
        List<BackupVerificationResult> verificationResults =
                backupVerification.verifyAllBackups(BackupVersion.SNAPSHOT_META_SERVICE, dateRange);
        if (!verificationResults.isEmpty()
                && verificationResults
                                .stream()
                                .filter(backupVerificationResult -> backupVerificationResult.valid)
                                .count()
                        == 0) {
            logger.error(
                    "Not able to find any snapshot which is valid in our SLO window: {} hours",
                    backupRestoreConfig.getBackupVerificationSLOInHours());
            backupMetrics.incrementBackupVerificationFailure();
        } else {
            // we would be here if there are no backup verification results
            // or backup verification results are available and are all valid.
            // send notifications for each backup that was uploaded and verified.
            verificationResults
                    .stream()
                    .forEach(
                            backupVerificationResult -> {
                                logger.info(
                                        "Sending {} message for backup: {}",
                                        AbstractBackupPath.BackupFileType.SNAPSHOT_VERIFIED,
                                        backupVerificationResult.snapshotInstant);
                                backupNotificationMgr.notify(backupVerificationResult);
                            });
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
