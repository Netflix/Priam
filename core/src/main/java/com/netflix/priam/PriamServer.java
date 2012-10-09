package com.netflix.priam;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.aws.UpdateCleanupPolicy;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.NodeRepairConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.noderepair.NodeRepairScheduler;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.noderepair.NodeRepair;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TuneCassandra;
import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
    private final NodeRepairConfiguration nodeRepairConfig;
    private final InstanceIdentity id;
    private final Sleeper sleeper;
    private static final Logger logger = LoggerFactory.getLogger(PriamServer.class);

    private static final Boolean NODEREPAIR = true;
    private Scheduler repairScheduler;

    @Inject
    public PriamServer(CassandraConfiguration cassandraConfig,
                       BackupConfiguration backupConfig,
                       AmazonConfiguration amazonConfig,
                       NodeRepairConfiguration nodeRepairConfig,
                       PriamScheduler scheduler,
                       InstanceIdentity id,
                       Sleeper sleeper) {
        this.cassandraConfig = cassandraConfig;
        this.backupConfig = backupConfig;
        this.amazonConfig = amazonConfig;
        this.nodeRepairConfig = nodeRepairConfig;
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

        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);

        // restore from backup else start cassandra.
        if (StringUtils.isNotBlank(backupConfig.getAutoRestoreSnapshotName())) {
            //scheduler.addTask(Restore.JOBNAME, Restore.class, Restore.getTimer());
            scheduler.addTask(Restore.getJobDetail(), Restore.getTrigger());
        } else {
            SystemUtils.startCassandra(true, cassandraConfig, backupConfig, amazonConfig.getInstanceType()); // Start cassandra.
        }

        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1)
        if (backupConfig.getHour() >= 0 && (CollectionUtils.isEmpty(backupConfig.getAvailabilityZonesToBackup()) || backupConfig.getAvailabilityZonesToBackup().contains(amazonConfig.getAvailabilityZone()))) {
            //scheduler.addTask(SnapshotBackup.JOBNAME, SnapshotBackup.class, SnapshotBackup.getTimer(backupConfig));
            scheduler.addTask(SnapshotBackup.getJobDetail(), SnapshotBackup.getTrigger(backupConfig));

            // Start the Incremental backup schedule if enabled
            if (backupConfig.isIncrementalEnabled()) {
                //scheduler.addTask(IncrementalBackup.JOBNAME, IncrementalBackup.class, IncrementalBackup.getTimer());
                scheduler.addTask(IncrementalBackup.getJobDetail(), IncrementalBackup.getTrigger());
            }
        }

        //Set cleanup
        //scheduler.addTask(UpdateCleanupPolicy.JOBNAME, UpdateCleanupPolicy.class, UpdateCleanupPolicy.getTimer());
        scheduler.addTask(UpdateCleanupPolicy.getJobDetail(), UpdateCleanupPolicy.getTrigger());

        //Schedule Node Repair
        //Do it a bit differently because we need to build schedule for each keyspace separately by passing keyspace name as an argument
        if(NODEREPAIR){
            try{
                new NodeRepairScheduler().scheduleNodeRepair();
            } catch (Exception e) {
                logger.error("%s", e.getMessage());
            }
        }
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
