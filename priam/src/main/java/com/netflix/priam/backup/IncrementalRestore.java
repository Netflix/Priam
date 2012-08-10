package com.netflix.priam.backup;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.Sleeper;
import org.apache.cassandra.io.sstable.SSTableLoaderWrapper;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Calendar;
import java.util.Iterator;

/*
 * Incremental SSTable Restore using SSTable Loader
 */
@Singleton
public class IncrementalRestore extends AbstractRestore
{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalRestore.class);
    public static final String JOBNAME = "INCR_RESTORE_THREAD";
    private final File restoreDir;
    
    @Inject
    private SSTableLoaderWrapper loader;
    
    @Inject
    private PriamServer priamServer;

    @Inject
    public IncrementalRestore(IConfiguration config, Sleeper sleeper)
    {
        super(config, JOBNAME, sleeper);
        this.restoreDir = new File(config.getDataFileLocation(), "restore_incremental");
    }

    @Override
    public void execute() throws Exception
    {
        String prefix = config.getRestorePrefix();
        if (Strings.isNullOrEmpty(prefix))
        {
            logger.error("Restore prefix is not set, skipping incremental restore to avoid looping over the incremental backups. Plz check the configurations");
            return; // No point in restoring the files which was just backedup.
        }

        if (config.isRestoreClosestToken())
        {
            priamServer.getId().getInstance().setToken(restoreToken.toString());
        }

        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, latest.time, Calendar.getInstance().getTime());
        FileUtils.createDirectory(restoreDir); // create restore dir.
        while (incrementals.hasNext())
        {
            latest = incrementals.next();
            // skip System informations.
            if (latest.getType() != BackupFileType.SST || latest.getKeyspace().equalsIgnoreCase("System"))
                continue;
            
            File keyspaceDir = new File(restoreDir, latest.keyspace);
            FileUtils.createDirectory(keyspaceDir);
            download(latest, new File(keyspaceDir, latest.fileName));
        }
        // wait for all the downloads in this batch to complete.
        waitToComplete();
        
        // stream the SST's in the dir
        for (File keyspaceDir : restoreDir.listFiles())
        {
            loader.stream(keyspaceDir);
            // cleanup the dir which where streamed.
            loader.deleteCompleted();
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
