package com.netflix.priam;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.aws.UpdateCleanupPolicy;
import com.netflix.priam.aws.UpdateSecuritySettings;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TuneCassandra;
import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Start all tasks here - Property update task - Backup task - Restore task -
 * Incremental backup
 */
@Singleton
public class PriamServer implements Managed {
    private final PriamScheduler scheduler;
    private final CassandraConfiguration cassandraConfig;
    private final BackupConfiguration backupConfig;
    private final AmazonConfiguration amazonConfig;
    private final InstanceIdentity id;
    private final Sleeper sleeper;

    @Inject
    public PriamServer(CassandraConfiguration cassandraConfig,
                       BackupConfiguration backupConfig,
                       AmazonConfiguration amazonConfig,
                       PriamScheduler scheduler,
                       InstanceIdentity id,
                       Sleeper sleeper) {
        this.cassandraConfig = cassandraConfig;
        this.backupConfig = backupConfig;
        this.amazonConfig = amazonConfig;
        this.scheduler = scheduler;
        this.id = id;
        this.sleeper = sleeper;
    }

    @Override
    public void start() throws Exception {
        if (id.getInstance().isOutOfService()) {
            return;
        }

        // start to schedule jobs
        scheduler.start();

        // update security settings.
        if (cassandraConfig.isMultiRegionEnabled()) {
            scheduler.runTaskNow(UpdateSecuritySettings.class);
            // sleep for 60 sec for the SG update to happen.
            if (UpdateSecuritySettings.firstTimeUpdated) {
                sleeper.sleep(60 * 1000);
            }
            scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer(id));
        }

        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);

        // restore from backup else start cassandra.
        if (StringUtils.isNotBlank(backupConfig.getAutoRestoreSnapshotName())) {
            scheduler.addTask(Restore.JOBNAME, Restore.class, Restore.getTimer());
        } else {
            SystemUtils.startCassandra(true, cassandraConfig, backupConfig, amazonConfig.getInstanceType()); // Start cassandra.
        }

        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1)
        if (backupConfig.getHour() >= 0 && (CollectionUtils.isEmpty(backupConfig.getAvailabilityZonesToBackup()) || backupConfig.getAvailabilityZonesToBackup().contains(amazonConfig.getAvailabilityZone()))) {
            scheduler.addTask(SnapshotBackup.JOBNAME, SnapshotBackup.class, SnapshotBackup.getTimer(backupConfig));

            // Start the Incremental backup schedule if enabled
            if (backupConfig.isIncrementalEnabled()) {
                scheduler.addTask(IncrementalBackup.JOBNAME, IncrementalBackup.class, IncrementalBackup.getTimer());
            }
        }

        //Set cleanup
        scheduler.addTask(UpdateCleanupPolicy.JOBNAME, UpdateCleanupPolicy.class, UpdateCleanupPolicy.getTimer());
    }

    @Override
    public void stop() throws Exception {
        scheduler.shutdown();
    }

    public InstanceIdentity getInstanceIdentity() {
        return id;
    }

    public PriamScheduler getScheduler() {
        return scheduler;
    }
}
