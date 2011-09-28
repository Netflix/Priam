package com.priam.backup;

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
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.conf.IConfiguration;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.InstanceIdentity;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.Task;
import com.priam.scheduler.TaskTimer;
import com.priam.utils.RetryableCallable;
import com.priam.utils.SystemUtils;

@Singleton
public class Restore extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private AtomicInteger count = new AtomicInteger();
    private IConfiguration config;
    private ThreadPoolExecutor executor;
    private IBackupFileSystem fs;

    @Inject
    Provider<AbstractBackupPath> pathProvider;
    @Inject
    MetaData metaData;

    @Inject
    public Restore(IConfiguration config, IPriamInstanceFactory factory, IBackupFileSystem fs)
    {
        this.config = config;
        this.fs = fs;
        this.executor = new ThreadPoolExecutor(0, config.getMaxBackupDownloadThreads(), 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public void execute() throws Exception
    {
        if (!"".equals(config.getRestoreSnapshot()))
        {
            String[] restore = config.getRestoreSnapshot().split(",");
            AbstractBackupPath path = pathProvider.get();
            Date startTime = path.getFormat().parse(restore[0]);
            Date endTime = path.getFormat().parse(restore[1]);
            restore(startTime, endTime);
        }
        SystemUtils.startCassandra(true, config);
    }

    public void restore(Date startTime, Date endTime) throws Exception
    {
        // Try and read the Meta file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        Iterator<AbstractBackupPath> backupfiles = fs.list(config.getBackupPrefix(), startTime, endTime);
        while (backupfiles.hasNext())
        {
            AbstractBackupPath path = backupfiles.next();
            if (path.type == BackupFileType.META)
                metas.add(path);
        }
        assert metas.size() != 0 : "[cass_backup] No snapshots found, Restore Failed.";

        Collections.sort(metas);
        AbstractBackupPath meta = metas.get(metas.size() - 1);
        // Download the snapshot which are listed in the meta file.
        // Don't download all of them but download the latest only.
        List<AbstractBackupPath> snapshots = metaData.get(meta);
        for (AbstractBackupPath path : snapshots)
            download(path);

        // take care of the incremental's.
        Iterator<AbstractBackupPath> incrementals = fs.list(config.getBackupPrefix(), meta.time, endTime);
        while (incrementals.hasNext())
        {
            AbstractBackupPath path = incrementals.next();
            if (path.type == BackupFileType.SST)
                download(path);
        }
        waitToComplete();
        // TODO support restore of the commit log.
    }

    public void download(final AbstractBackupPath path)
    {
        count.incrementAndGet();
        executor.submit(new RetryableCallable<Integer>()
        {
            @Override
            public Integer retriableCall() throws Exception
            {
                fs.download(path);
                return count.decrementAndGet();
            }
        });
    }

    public void waitToComplete()
    {
        // TODO timeout? here or individual file download?
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
        return executor.getActiveCount();
    }

}
