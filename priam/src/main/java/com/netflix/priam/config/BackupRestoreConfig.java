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

import com.netflix.priam.configSource.IConfigSource;
import javax.inject.Inject;

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
}
