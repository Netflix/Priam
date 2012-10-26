package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.CassandraConfiguration;
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


    @Override
    public String getName() {
        return JOBNAME;
    }

    @Override
    public long getIntervalInMilliseconds(){
        return 10L * 1000;
    }

    public String getTriggerName(){
        return "incrementalbackup-trigger";
    }

}
