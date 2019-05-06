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
package com.netflix.priam.tuner;

import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.RetryableCallable;
import javax.inject.Inject;

public class CassandraTunerService implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration configuration;
    private final IBackupRestoreConfig backupRestoreConfig;

    @Inject
    public CassandraTunerService(
            PriamScheduler priamScheduler,
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig) {
        this.scheduler = priamScheduler;
        this.configuration = configuration;
        this.backupRestoreConfig = backupRestoreConfig;
    }

    @Override
    public void scheduleService() throws Exception {
        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);
    }

    @Override
    public void updateServicePre() throws Exception {}

    @Override
    public void updateServicePost() throws Exception {
        // Update the cassandra to enable/disable new incremental files.
        new RetryableCallable<Void>(6, 10000) {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                nodetool.setIncrementalBackupsEnabled(
                        IncrementalBackup.isEnabled(configuration, backupRestoreConfig));
                return null;
            }
        }.call();
    }
}
