/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam;

import org.apache.commons.collections.CollectionUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.aws.UpdateCleanupPolicy;
import com.netflix.priam.aws.UpdateSecuritySettings;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TuneCassandra;

/**
 * Start all tasks here - Property update task - Backup task - Restore task -
 * Incremental backup
 */
@Singleton
public class PriamServer
{
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private final InstanceIdentity id;
    private final Sleeper sleeper;
    private final ICassandraProcess cassProcess;
    private static final int CASSANDRA_MONITORING_INITIAL_DELAY = 10;

    @Inject
    public PriamServer(IConfiguration config, PriamScheduler scheduler, InstanceIdentity id, Sleeper sleeper, ICassandraProcess cassProcess)
    {
        this.config = config;
        this.scheduler = scheduler;
        this.id = id;
        this.sleeper = sleeper;
        this.cassProcess = cassProcess;
    }

    public void intialize() throws Exception
    {     
        if (id.getInstance().isOutOfService())
            return;

        // start to schedule jobs
        scheduler.start();

        // update security settings.
        if (config.isMultiDC())
        {
            scheduler.runTaskNow(UpdateSecuritySettings.class);
            // sleep for 60 sec for the SG update to happen.
            if (UpdateSecuritySettings.firstTimeUpdated)
                sleeper.sleep(60 * 1000);
            scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer(id));
        }

        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);

        // restore from backup else start cassandra.
        if (!config.getRestoreSnapshot().equals(""))
            scheduler.addTask(Restore.JOBNAME, Restore.class, Restore.getTimer());
        else
            cassProcess.start(true);

        /*
         *  Run the delayed task (after 10 seconds) to Monitor Cassandra
         *  If Restore option is chosen, then Running Cassandra instance is stopped 
         *  Hence waiting for Cassandra to stop
         */
        scheduler.addTaskWithDelay(CassandraMonitor.JOBNAME,CassandraMonitor.class, CassandraMonitor.getTimer(), CASSANDRA_MONITORING_INITIAL_DELAY);

        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1)
        if (config.getBackupHour() >= 0 && (CollectionUtils.isEmpty(config.getBackupRacs()) || config.getBackupRacs().contains(config.getRac())))
        {
            scheduler.addTask(SnapshotBackup.JOBNAME, SnapshotBackup.class, SnapshotBackup.getTimer(config));

            // Start the Incremental backup schedule if enabled
            if (config.isIncrBackup())
                scheduler.addTask(IncrementalBackup.JOBNAME, IncrementalBackup.class, IncrementalBackup.getTimer());
        }
        
        //Set cleanup
        scheduler.addTask(UpdateCleanupPolicy.JOBNAME, UpdateCleanupPolicy.class, UpdateCleanupPolicy.getTimer());
    }

    public InstanceIdentity getId()
    {
        return id;
    }

    public PriamScheduler getScheduler()
    {
        return scheduler;
    }

    public IConfiguration getConfiguration()
    {
        return config;
    }

}
