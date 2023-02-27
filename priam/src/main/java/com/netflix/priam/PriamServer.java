/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam;

import com.netflix.priam.backup.BackupService;
import com.netflix.priam.backupv2.BackupV2Service;
import com.netflix.priam.cluster.management.ClusterManagementService;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.config.PriamConfigurationPersister;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.health.CassandraMonitor;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.restore.RestoreContext;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.tuner.CassandraTunerService;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Start all tasks here - Property update task - Backup task - Restore task - Incremental backup */
@Singleton
public class PriamServer implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private final InstanceIdentity instanceIdentity;
    private final Sleeper sleeper;
    private final ICassandraProcess cassProcess;
    private final RestoreContext restoreContext;
    private final IService backupV2Service;
    private final IService backupService;
    private final IService cassandraTunerService;
    private final IService clusterManagementService;
    private static final int CASSANDRA_MONITORING_INITIAL_DELAY = 10;
    private static final Logger logger = LoggerFactory.getLogger(PriamServer.class);

    @Inject
    public PriamServer(
            IConfiguration config,
            PriamScheduler scheduler,
            InstanceIdentity id,
            Sleeper sleeper,
            ICassandraProcess cassProcess,
            RestoreContext restoreContext,
            BackupService backupService,
            BackupV2Service backupV2Service,
            CassandraTunerService cassandraTunerService,
            ClusterManagementService clusterManagementService) {
        this.config = config;
        this.scheduler = scheduler;
        this.instanceIdentity = id;
        this.sleeper = sleeper;
        this.cassProcess = cassProcess;
        this.restoreContext = restoreContext;
        this.backupService = backupService;
        this.backupV2Service = backupV2Service;
        this.cassandraTunerService = cassandraTunerService;
        this.clusterManagementService = clusterManagementService;
    }

    private void createDirectories() throws IOException {
        SystemUtils.createDirs(config.getBackupCommitLogLocation());
        SystemUtils.createDirs(config.getCommitLogLocation());
        SystemUtils.createDirs(config.getCacheLocation());
        SystemUtils.createDirs(config.getDataFileLocation());
        SystemUtils.createDirs(config.getLogDirLocation());
    }

    @Override
    public void scheduleService() throws Exception {
        // Create all the required directories for priam and Cassandra.
        createDirectories();

        // Do not start Priam if you are out of service.
        if (instanceIdentity.getInstance().isOutOfService()) return;

        // start to schedule jobs
        scheduler.start();

        // Set up cassandra tuning.
        cassandraTunerService.scheduleService();

        // Determine if we need to restore from backup else start cassandra.
        if (restoreContext.isRestoreEnabled()) {
            restoreContext.restore();
        } else { // no restores needed
            logger.info("No restore needed, task not scheduled");
            if (!config.doesCassandraStartManually()) cassProcess.start(true); // Start cassandra.
            else
                logger.info(
                        "config.doesCassandraStartManually() is set to True, hence Cassandra needs to be started manually ...");
        }

        /*
         *  Run the delayed task (after 10 seconds) to Monitor Cassandra
         *  If Restore option is chosen, then Running Cassandra instance is stopped
         *  Hence waiting for Cassandra to stop
         */
        scheduler.addTaskWithDelay(
                CassandraMonitor.JOBNAME,
                CassandraMonitor.class,
                CassandraMonitor.getTimer(),
                CASSANDRA_MONITORING_INITIAL_DELAY);

        // Set up management services like flush, compactions etc.
        clusterManagementService.scheduleService();

        // Set up the background configuration dumping thread
        scheduleTask(
                scheduler,
                PriamConfigurationPersister.class,
                PriamConfigurationPersister.getTimer(config));

        // Set up V1 Snapshot Service
        backupService.scheduleService();

        // Set up V2 Snapshot Service
        backupV2Service.scheduleService();
    }

    @Override
    public void updateServicePre() throws Exception {}

    @Override
    public void updateServicePost() throws Exception {}

    public InstanceIdentity getInstanceIdentity() {
        return instanceIdentity;
    }

    public PriamScheduler getScheduler() {
        return scheduler;
    }

    public IConfiguration getConfiguration() {
        return config;
    }
}
