/**
 * Copyright 2018 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.config;

import com.google.inject.ImplementedBy;

/**
 * This interface is to abstract out the backup and restore configuration used by Priam. Goal is to
 * eventually have each module/functionality to have its own Config. Created by aagrawal on 6/26/18.
 */
@ImplementedBy(BackupRestoreConfig.class)
public interface IBackupRestoreConfig {

    /**
     * Cron expression to be used for snapshot meta service. Use "-1" to disable the service.
     *
     * @return Snapshot Meta Service cron expression for generating manifest.json
     * @see <a
     *     href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a>
     */
    default String getSnapshotMetaServiceCronExpression() {
        return "-1";
    }

    /**
     * Enable the backup version 2.0 in new format. This will start uploads of "incremental" backups
     * in new format. This is to be used for migration from backup version 1.0.
     *
     * @return boolean value indicating if backups in version 2.0 should be started.
     */
    default boolean enableV2Backups() {
        return false;
    }

    /**
     * Cron expression to be used for the service which does TTL of the backups. This service will
     * run only if v2 backups are enabled. The idea is to run this service at least once a day to
     * ensure we are marking backup files for TTL as configured via {@link
     * IConfiguration#getBackupRetentionDays()}
     *
     * @return Backup TTL Service cron expression for trying to delete backups. Note that this CRON
     *     is only the job trying to delete backups and is not the TTL of the backups.
     * @see <a
     *     href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a>
     */
    default String getBackupTTLCronExpression() {
        return "0 0 0/6 1/1 * ? *";
    }

    /**
     * If restore is enabled and if this flag is enabled, we will try to restore using Backup V2.0.
     *
     * @return if restore should be using backup version 2.0. If this is false we will use backup
     *     version 1.0.
     */
    default boolean enableV2Restore() {
        return false;
    }
}
