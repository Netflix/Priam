package com.priam.backup;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.conf.IConfiguration;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.TaskTimer;

/*
 * Incremental/SSTable backup
 */
@Singleton
public class IncrementalBackup extends Backup
{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    public static final String JOBNAME = "INCR_BACKUP_THREAD";
    private IConfiguration config;

    @Inject
    public IncrementalBackup(IConfiguration config)
    {
        this.config = config;
    }

    @Override
    public void execute() throws Exception
    {
        File dataDir = new File(config.getDataFileLocation());
        logger.debug("Scanning for backup in: " + dataDir.getAbsolutePath());
        for (File keyspaceDir : dataDir.listFiles())
        {
            File backupDir = new File(keyspaceDir, "backups");
            if (!isValidBackupDir(keyspaceDir, backupDir))
                continue;
            upload(backupDir, BackupFileType.SST);
        }
    }

    /**
     * Run every 10 Sec
     */
    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

}
