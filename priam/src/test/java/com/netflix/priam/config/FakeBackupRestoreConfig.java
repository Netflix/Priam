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

import com.google.common.collect.ImmutableSet;

/** Created by aagrawal on 6/26/18. */
public class FakeBackupRestoreConfig implements IBackupRestoreConfig {
    @Override
    public String getSnapshotMetaServiceCronExpression() {
        return "0 0/2 * 1/1 * ? *"; // Every 2 minutes for testing purposes
    }

    @Override
    public boolean enableV2Backups() {
        return false;
    }

    @Override
    public boolean enableV2Restore() {
        return false;
    }

    @Override
    public int getBackupTTLMonitorPeriodInSec() {
        return 0; // avoids sleeping altogether in tests.
    }

    @Override
    public ImmutableSet<String> getBackupNotificationAdditionalMessageAttrs() {
        return ImmutableSet.of();
    }
}
