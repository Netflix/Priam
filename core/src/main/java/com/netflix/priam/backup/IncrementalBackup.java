package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;



/*
 * Incremental/SSTable backup
 */
@Singleton
public class IncrementalBackup extends AbstractBackup {
    public static final String JOBNAME = "INCR_BACKUP_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    private final CassandraConfiguration cassandraConfiguration;

    @Inject
    public IncrementalBackup(CassandraConfiguration cassandraConfiguration, IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory) {
        super(fs, pathFactory);
        this.cassandraConfiguration = cassandraConfiguration;
    }

    @Override
    public void execute() throws Exception {
        File dataDir = new File(cassandraConfiguration.getDataLocation());
        logger.debug("Scanning for backup in: " + dataDir.getAbsolutePath());
        File[] keyspaceDirs = dataDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });
        for (File keyspaceDir : keyspaceDirs) {
            for (File columnFamilyDir : keyspaceDir.listFiles()) {
                File backupDir = new File(columnFamilyDir, "backups");
                if (!isValidBackupDir(keyspaceDir, columnFamilyDir, backupDir)) {
                    continue;
                }
                upload(backupDir, BackupFileType.SST);
            }
        }
    }

    /**
     * Run every 10 Sec
     */
    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static JobDetail getJobDetail(){
        JobDetail jobDetail = JobBuilder.newJob(IncrementalBackup.class)
                .withIdentity("priam-scheduler", "incremental-backup")
                .build();
        return jobDetail;
    }

    public static Trigger getTrigger(){
        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("priam-scheduler", "incremental-backup-trigger")
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(10L * 1000).repeatForever())
                .build();
        return trigger;
    }

}
