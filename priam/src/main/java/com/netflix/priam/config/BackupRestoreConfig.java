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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.configSource.IConfigSource;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;

/** Implementation of IBackupRestoreConfig. Created by aagrawal on 6/26/18. */
public class BackupRestoreConfig implements IBackupRestoreConfig {

    private final IConfigSource config;

    @Inject
    public BackupRestoreConfig(IConfigSource config) {
        this.config = config;
    }

    @Override
    public String getSnapshotMetaServiceCronExpression() {
        return config.get("priam.snapshot.meta.cron", "-1");
    }

    @Override
    public boolean enableV2Backups() {
        return config.get("priam.enableV2Backups", false);
    }

    @Override
    public boolean enableV2Restore() {
        return config.get("priam.enableV2Restore", false);
    }

    @Override
    public int getBackupTTLMonitorPeriodInSec() {
        return config.get("priam.backupTTLMonitorPeriodInSec", 21600);
    }

    @Override
    public int getBackupVerificationSLOInHours() {
        return config.get("priam.backupVerificationSLOInHours", 24);
    }

    @Override
    public String getBackupVerificationCronExpression() {
        return config.get("priam.backupVerificationCronExpression", "0 30 0/1 1/1 * ? *");
    }

    @Override
    public String getBackupNotifyComponentIncludeList() {
        return config.get("priam.backupNotifyComponentIncludeList", StringUtils.EMPTY);
    }

    @Override
    public ImmutableSet<String> getBackupNotificationAdditionalMessageAttrs() {
        String value = config.get("priam.backupNotifyAdditionalMessageAttrs", StringUtils.EMPTY);
        return ImmutableSet.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(value));
    }
}
