package com.netflix.priam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.priam.aws.UpdateSecuritySettings;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TuneCassandra;

/**
 * Start all tasks here
 *  - Property update task
 *  - Backup task
 *  - Restore task
 *  - Incremental backup
 */
public class PriamServer
{
    public static final PriamServer instance = new PriamServer();
    public Injector injector;
    public PriamScheduler scheduler;
    public IConfiguration config;
    public InstanceIdentity id;

    private PriamServer()
    {
    }

    public void intialize(Module module) throws Exception
    {
        injector = Guice.createInjector(module);
        config = injector.getInstance(IConfiguration.class);
        // Initialize the configurations to be read.
        config.intialize();
        id = injector.getInstance(InstanceIdentity.class);
        if( id.getInstance().isOutOfService())
            return;
        scheduler = injector.getInstance(PriamScheduler.class);
        // start to schedule jobs
        scheduler.start();

        // update security settings.
        if (config.isMultiDC())
        {
            scheduler.runTaskNow(UpdateSecuritySettings.class);
            // sleep for 60 sec for the SG update to happen.
            if(UpdateSecuritySettings.firstTimeUpdated)
                Thread.sleep(60 * 1000);
            scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer());
        }

        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);

        // restore from backup else start cassandra.
        if (!config.getRestoreSnapshot().equals(""))
            scheduler.addTask(Restore.JOBNAME, Restore.class, Restore.getTimer());
        else
            SystemUtils.startCassandra(true, config); // Start cassandra.

        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1)
        if (config.getBackupHour() >= 0){
            scheduler.addTask(SnapshotBackup.JOBNAME, SnapshotBackup.class, SnapshotBackup.getTimer(config));

            // Start the Incremental backup schedule if enabled
            if (config.isIncrBackup())
                scheduler.addTask(IncrementalBackup.JOBNAME, IncrementalBackup.class, IncrementalBackup.getTimer());
        }
    }

}
