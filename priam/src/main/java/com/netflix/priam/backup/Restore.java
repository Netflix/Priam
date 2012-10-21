package com.netflix.priam.backup;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Main class for restoring data from backup
 */
@Singleton
public class Restore extends AbstractRestore
{
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    @Inject
    private Provider<AbstractBackupPath> pathProvider;
    @Inject
    private RestoreTokenSelector tokenSelector;
    @Inject
    private MetaData metaData;
    @Inject
    private PriamServer priamServer;

    @Inject
    public Restore(IConfiguration config, Sleeper sleeper)
    {
        super(config, JOBNAME, sleeper);
    }

    @Override
    public void execute() throws Exception
    {
        if (isRestoreEnabled(config))
        {
            logger.info("Starting restore for " + config.getRestoreSnapshot());
            String[] restore = config.getRestoreSnapshot().split(",");
            AbstractBackupPath path = pathProvider.get();
            final Date startTime = path.getFormat().parse(restore[0]);
            final Date endTime = path.getFormat().parse(restore[1]);
            String origToken = priamServer.getId().getInstance().getToken();
            try
            {
                if (config.isRestoreClosestToken())
                {
                    restoreToken = tokenSelector.getClosestToken(new BigInteger(origToken), startTime);
                    priamServer.getId().getInstance().setToken(restoreToken.toString());
                }
                new RetryableCallable<Void>()
                {
                    public Void retriableCall() throws Exception
                    {
                        logger.info("Attempting restore");
                        restore(startTime, endTime);
                        logger.info("Restore completed");
                        // Wait for other server init to complete
                        sleeper.sleep(30000);
                        return null;
                    }
                }.call();
            }
            finally
            {
                priamServer.getId().getInstance().setToken(origToken);
            }
        }
        SystemUtils.startCassandra(true, config);
    }

    /**
     * Restore backup data for the specified time range
     */
    public void restore(Date startTime, Date endTime) throws Exception
    {
        // Stop cassandra if its running and restoring all keyspaces
        if (config.getRestoreKeySpaces().size() == 0)
            SystemUtils.stopCassandra(config);

        // Cleanup local data
        SystemUtils.cleanupDir(config.getDataFileLocation(), config.getRestoreKeySpaces());

        // Try and read the Meta file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        String prefix = "";
        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();
        logger.info("Looking for meta file here:  " + prefix);
        Iterator<AbstractBackupPath> backupfiles = fs.list(prefix, startTime, endTime);
        while (backupfiles.hasNext())
        {
            AbstractBackupPath path = backupfiles.next();
            if (path.type == BackupFileType.META)
            {
            		metas.add(path);
            		//path.parseLocal(file, BackupFileType.META);
            }
        }
        logger.info("metas.size() =  " + metas.size());
        assert metas.size() != 0 : "[cass_backup] No snapshots found, Restore Failed.";

        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Meta file for restore " + meta.getRemotePath());

        // Download snapshot which is listed in the meta file.
        List<AbstractBackupPath> snapshots = metaData.get(meta);
        for(AbstractBackupPath abPath: snapshots)
        		logger.info("*** Snapshot --> "+abPath.getRemotePath());
        download(snapshots.iterator(), BackupFileType.SNAP);

        logger.info("Downloading incrementals");
        // Download incrementals (SST).
        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.time, endTime);
        download(incrementals, BackupFileType.SST);
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public int getActiveCount()
    {
        return (executor == null) ? 0 : executor.getActiveCount();
    }

    public static boolean isRestoreEnabled(IConfiguration conf)
    {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac = (CollectionUtils.isEmpty(conf.getBackupRacs()) || conf.getBackupRacs().contains(conf.getRac()));
        return (isRestoreMode && isBackedupRac);
    }

}
