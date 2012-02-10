package com.netflix.priam.backup;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.netflix.priam.utils.ExponentialRetryCallable;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;

/** 
 * Main class for restoring data from backup
 */
@Singleton
public class Restore extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    public static final String SYSTEM_KEYSPACE = "system";
    private AtomicInteger count = new AtomicInteger();
    private IConfiguration config;
    private ThreadPoolExecutor executor;
    private IBackupFileSystem fs;

    @Inject
    Provider<AbstractBackupPath> pathProvider;
    @Inject
    Provider<IRestoreTokenSelector> tokenSelectorProvider;
    @Inject
    MetaData metaData;

    @Inject
    public Restore(IConfiguration config, IBackupFileSystem fs)
    {
        this.config = config;
        this.fs = fs;
    }

    @Override
    public void execute() throws Exception
    {
        if (!"".equals(config.getRestoreSnapshot()))
        {
            logger.info("Starting restore for " + config.getRestoreSnapshot());
            String[] restore = config.getRestoreSnapshot().split(",");
            AbstractBackupPath path = pathProvider.get();
            final Date startTime = path.getFormat().parse(restore[0]);
            final Date endTime = path.getFormat().parse(restore[1]);
            String origToken = PriamServer.instance.id.getInstance().getPayload();
            try
            {
                if (config.isRestoreClosestToken())
                {
                    BigInteger restoreToken = tokenSelectorProvider.get().getClosestToken(new BigInteger(origToken), startTime);
                    PriamServer.instance.id.getInstance().setPayload(restoreToken.toString());
                }
                new RetryableCallable<Void>()
                {
                    public Void retriableCall() throws Exception
                    {
                        logger.info("Attempting restore");
                        restore(startTime, endTime);
                        logger.info("Restore completed");
                        return null;
                    }
                }.call();
            }
            finally
            {
                PriamServer.instance.id.getInstance().setPayload(origToken);
            }
        }
        SystemUtils.startCassandra(true, config);
    }

    /**
     * Restore backup data for the specified time range
     * @param startTime
     * @param endTime
     * @throws Exception
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

            // Download the snapshot which is listed in the meta file.
            List<AbstractBackupPath> snapshots = metaData.get(meta);
            for (AbstractBackupPath path : snapshots)
                download(path);

            // take care of the incrementals.
            Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.time, endTime);
            if (config.isCommitLogBackup())
                downloadCommitLogs(incrementals);
            else
                downloadIncrementals(incrementals, BackupFileType.SST);
        }
        finally
        {
            executor.shutdownNow();
        }

    }

    private void downloadIncrementals(Iterator<AbstractBackupPath> incrementals, BackupFileType fileType) throws Exception
    {
        while (incrementals.hasNext())
        {
            AbstractBackupPath path = incrementals.next();
            if (path.type == fileType)
                download(path);
        }
        waitToComplete();
    }

    private void downloadCommitLogs(Iterator<AbstractBackupPath> incrementals) throws Exception
    {
        waitToComplete();
        SystemUtils.startCassandra(false, config);
        waitForCassandra();
        logger.info("Downloading incremental commitlogs");
        downloadIncrementals(incrementals, BackupFileType.CL);
        waitToComplete();
        JMXNodeTool.instance(config).joinRing();
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
                //Need to handle Commit logs here
                fs.download(path);
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

    private void waitForCassandra() throws Exception
    {
        new ExponentialRetryCallable<Void>(3000, 10 * 60 * 1000)
        {
            @Override
            public Void retriableCall() throws Exception
            {
                logger.info("Waiting for cassandra to start...");
                JMXNodeTool.instance(config).info();//Got Better check?
                return null;
            }
        }.call();
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
        return executor.getActiveCount();
    }

}
