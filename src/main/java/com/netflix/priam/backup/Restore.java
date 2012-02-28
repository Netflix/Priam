package com.netflix.priam.backup;

import java.io.FileOutputStream;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.PriamServer;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;

/**
 * Main class for restoring data from backup
 */
@Singleton
public class Restore extends Task
{
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final String SYSTEM_KEYSPACE = "system";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    private AtomicInteger count = new AtomicInteger();
    private ThreadPoolExecutor executor;
    private final IBackupFileSystem fs;
    private final Provider<AbstractBackupPath> pathProvider;
    private final RestoreTokenSelector tokenSelector;
    private final MetaData metaData;
    private final PriamServer priamServer;

    @Inject
    public Restore(IConfiguration config, IBackupFileSystem fs, Provider<AbstractBackupPath> pathProvider, RestoreTokenSelector tokenSelector, MetaData metaData, PriamServer priamServer)
    {
        super(config);
        this.pathProvider = pathProvider;
        this.tokenSelector = tokenSelector;
        this.metaData = metaData;
        this.fs = fs;
        this.priamServer = priamServer;
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
                    BigInteger restoreToken = tokenSelector.getClosestToken(new BigInteger(origToken), startTime);
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
                        Thread.sleep(30000);
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
        try
        {
            executor = new ThreadPoolExecutor(config.getMaxBackupDownloadThreads(), config.getMaxBackupDownloadThreads(), 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
            executor.allowCoreThreadTimeOut(true);
            // Stop cassandra if its running and restoring all keyspaces
            if (config.getRestoreKeySpaces().size() == 0)
                SystemUtils.stopCassandra(config);

            // Cleanup local data
            SystemUtils.cleanupDir(config.getDataFileLocation(), config.getRestoreKeySpaces());

            // Try and read the Meta file.
            List<AbstractBackupPath> metas = Lists.newArrayList();
            String prefix = "";
            if (!"".equals(config.getRestorePrefix()))
                prefix = config.getRestorePrefix();
            else
                prefix = config.getBackupPrefix();
            logger.info("Looking for meta file here:  " + prefix);
            Iterator<AbstractBackupPath> backupfiles = fs.list(prefix, startTime, endTime);
            while (backupfiles.hasNext())
            {
                AbstractBackupPath path = backupfiles.next();
                if (path.type == BackupFileType.META)
                    metas.add(path);
            }
            assert metas.size() != 0 : "[cass_backup] No snapshots found, Restore Failed.";

            Collections.sort(metas);
            AbstractBackupPath meta = metas.get(metas.size() - 1);
            logger.info("Meta file for restore " + meta.getRemotePath());

            // Download snapshot which is listed in the meta file.
            List<AbstractBackupPath> snapshots = metaData.get(meta);
            download(snapshots.iterator(), BackupFileType.SNAP);

            logger.info("Downloading incrementals");
            // Download incrementals (SST).
            Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.time, endTime);
            download(incrementals, BackupFileType.SST);
        }
        finally
        {
            executor.shutdownNow();
        }

    }

    private void download(Iterator<AbstractBackupPath> fileiter, BackupFileType filter) throws Exception
    {
        while (fileiter.hasNext())
        {
            AbstractBackupPath path = fileiter.next();
            if (path.getType() == filter)
                download(path);
        }
        waitToComplete();
    }

    public void download(final AbstractBackupPath path) throws Exception
    {
        if (config.getRestoreKeySpaces().size() != 0 && (!config.getRestoreKeySpaces().contains(path.keyspace) || path.keyspace.equals(SYSTEM_KEYSPACE)))
            return;

        count.incrementAndGet();
        executor.submit(new RetryableCallable<Integer>()
        {
            @Override
            public Integer retriableCall() throws Exception
            {
                fs.download(path, new FileOutputStream(path.newRestoreFile()));
                return count.decrementAndGet();
            }
        });
    }

    private void waitToComplete()
    {
        while (count.get() != 0)
        {
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted: ", e);
            }
        }
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
        return !(isRestoreMode && isBackedupRac);
    }

}
