package com.netflix.priam;

import com.google.inject.Guice;
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
    private final NodeRepairScheduler nodeRepairScheduler;
    private final InstanceIdentity id;
    private static final Logger logger = LoggerFactory.getLogger(PriamServer.class);

    @Inject
    public PriamServer(CassandraConfiguration cassandraConfig,
                       BackupConfiguration backupConfig,
                       AmazonConfiguration amazonConfig,
                       NodeRepairScheduler nodeRepairScheduler,
                       PriamScheduler scheduler,
                       InstanceIdentity id) {
        this.cassandraConfig = cassandraConfig;
        this.backupConfig = backupConfig;
        this.amazonConfig = amazonConfig;
        this.nodeRepairScheduler = nodeRepairScheduler;
        this.scheduler = scheduler;
        this.id = id;
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
            scheduler.addTask(Restore.getJobDetail(), Restore.getTrigger());
        } else {
            SystemUtils.startCassandra(true, cassandraConfig, backupConfig, amazonConfig.getInstanceType()); // Start cassandra.
        }

        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1)
         if (backupConfig.getHour() >= 0 && (CollectionUtils.isEmpty(backupConfig.getAvailabilityZonesToBackup()) || backupConfig.getAvailabilityZonesToBackup().contains(amazonConfig.getAvailabilityZone()))) {
            scheduler.addTask(SnapshotBackup.getJobDetail(), SnapshotBackup.getTrigger(backupConfig));

            // Start the Incremental backup schedule if enabled
            if (backupConfig.isIncrementalEnabled()) {
                scheduler.addTask(IncrementalBackup.getJobDetail(), IncrementalBackup.getTrigger());
            }
        }

        //Set cleanup
        scheduler.addTask(UpdateCleanupPolicy.getJobDetail(), UpdateCleanupPolicy.getTrigger());

        //Schedule Node Repair
        //Do it a bit differently because we need to pass several arguments to the job executor which Quartz doesn't allow by default.
        if(cassandraConfig.isNodeRepairEnabled()){
            try{
                nodeRepairScheduler.setJobFactory();
                nodeRepairScheduler.scheduleNodeRepair();
            } catch (Exception e) {
                throw new RuntimeException(e);
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
}
