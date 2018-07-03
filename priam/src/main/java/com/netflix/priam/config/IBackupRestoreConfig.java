/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.config;

import com.google.inject.ImplementedBy;

/**
 * This interface is to abstract out the backup and restore configuration used by Priam. Goal is to eventually have each module/functionality to have its own Config.
 * Created by aagrawal on 6/26/18.
 */
@ImplementedBy(BackupRestoreConfig.class)
public interface IBackupRestoreConfig {

    /**
     * Cron expression to be used for snapshot meta service.
     *
     * @return Snapshot Meta Service cron expression for generating manifest.json
     * @see <a href="http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html">quartz-scheduler</a>
     * @see <a href="http://www.cronmaker.com">http://www.cronmaker.com</a> To build new cron timer
     */
    public String getSnapshotMetaServiceCronExpression();
}
