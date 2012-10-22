package com.netflix.priam.backup;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Task for running daily snapshots
 */
@Singleton
public class SnapshotBackup extends AbstractBackup {
    public static String JOBNAME = "SnapshotBackup";

    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    private final MetaData metaData;
    private final CassandraConfiguration cassandraConfiguration;

    private static String snapShotBackUpCronTime;

    @Inject
    public SnapshotBackup(CassandraConfiguration cassandraConfiguration, BackupConfiguration backupConfiguration, IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory, MetaData metaData) {
        super(fs, pathFactory);
        this.cassandraConfiguration = cassandraConfiguration;
        this.metaData = metaData;
    }

    @Override
    public void execute() throws Exception {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        String snapshotName = pathFactory.get().getFormat().format(cal.getTime());
        try {
            logger.info("Starting snapshot " + snapshotName);
            takeSnapshot(snapshotName);
            // Collect all snapshot dir's under keyspace dir's
            List<AbstractBackupPath> backupPaths = Lists.newArrayList();
            File dataDir = new File(cassandraConfiguration.getDataLocation());

            if (dataDir.listFiles() == null) {
                logger.error("no keyspace dir exists in casssandra data dir. Snapshot quits !!");
                return;
            }

            for (File keyspaceDir : dataDir.listFiles()) {
                if (keyspaceDir.listFiles() == null) {
                    continue;
                }
                for (File columnFamilyDir : keyspaceDir.listFiles()) {
                    File snpDir = new File(columnFamilyDir, "snapshots");
                    if (!isValidBackupDir(keyspaceDir, columnFamilyDir, snpDir)) {
                        continue;
                    }
                    File snapshotDir = getValidSnapshot(columnFamilyDir, snpDir, snapshotName);
                    // Add files to this dir
                    if (null != snapshotDir) {
                        backupPaths.addAll(upload(snapshotDir, BackupFileType.SNAP));
                    }
                }
            }
            // Upload meta file
            metaData.set(backupPaths, snapshotName);
            logger.info("Snapshot upload complete for " + snapshotName);
        } finally {
            try {
                logger.info("clearing snapshot {}", snapshotName);
                clearSnapshot(snapshotName);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }
    }

    private File getValidSnapshot(File columnFamilyDir, File snpDir, String snapshotName) {
        for (File snapshotDir : snpDir.listFiles()) {
            if (snapshotDir.getName().matches(snapshotName)) {
                return snapshotDir;
            }
        }
        return null;
    }

    private void takeSnapshot(final String snapshotName) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
                nodetool.takeSnapshot(snapshotName, null, new String[0]);
                return null;
            }
        }.call();
    }

    private void clearSnapshot(final String snapshotTag) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
                nodetool.clearSnapshot(snapshotTag);
                return null;
            }
        }.call();
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static TaskTimer getTimer(BackupConfiguration config) {
        int hour = config.getHour();
        return new CronTimer(hour, 1, 0);
    }

    public static JobDetail getJobDetail(){
        JobDetail jobDetail = JobBuilder.newJob(SnapshotBackup.class)
                .withIdentity("priam-scheduler", "snapshotbackup")
                .build();
        return jobDetail;
    }

    public static Trigger getTrigger(BackupConfiguration config){
        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("priam-scheduler", "snapshotbackup-trigger")
                .withSchedule(CronScheduleBuilder.cronSchedule(config.getSnapShotBackUpCronTime()))
                .build();
        return trigger;
    }
}
