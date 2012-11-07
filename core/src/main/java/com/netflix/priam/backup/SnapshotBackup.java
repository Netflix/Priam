package com.netflix.priam.backup;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
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
    private final BackupConfiguration backupConfiguration;

    @Inject
    public SnapshotBackup(CassandraConfiguration cassandraConfiguration, BackupConfiguration backupConfiguration, IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory, MetaData metaData) {
        super(fs, pathFactory);
        this.cassandraConfiguration = cassandraConfiguration;
        this.backupConfiguration = backupConfiguration;
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
                logger.error("No keyspace directories exist in configured cassandra data directory. Unable to capture snapshot.");
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

    public String getTriggerName(){
        return "snapshotbackup-trigger";
    }

    @Override
    public String getCronTime(){
        return backupConfiguration.getSnapShotBackupCronTime();
    }

}
