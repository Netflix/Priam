package com.netflix.priam.backup;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;

public abstract class AbstractRestore extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestore.class);
    private static final String SYSTEM_KEYSPACE = "system";
    // keeps track of the last download which was executed.
    protected static volatile AbstractBackupPath latest;

    private AtomicInteger count = new AtomicInteger();
    
    protected IConfiguration config;
    protected ThreadPoolExecutor executor;
    
    @Inject
    protected IBackupFileSystem fs;
    public static BigInteger restoreToken;
    
    public AbstractRestore(IConfiguration config, String name)
    {
        super(config);
        this.config = config;
        executor = new JMXConfigurableThreadPoolExecutor(config.getMaxBackupDownloadThreads(), 
                                                         1000, 
                                                         TimeUnit.MILLISECONDS, 
                                                         new LinkedBlockingQueue<Runnable>(), 
                                                         new NamedThreadFactory(name), 
                                                         name);
        executor.allowCoreThreadTimeOut(true);
    }

    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter) throws Exception
    {
        while (fsIterator.hasNext())
        {
            latest = fsIterator.next();
            if (latest.getType() == filter)
                download(latest, latest.newRestoreFile());
        }
        waitToComplete();
    }

    /**
     * Download to specific location
     */
    public void download(final AbstractBackupPath path, final File restoreLocation) throws Exception
    {
        if (config.getRestoreKeySpaces().size() != 0 && (!config.getRestoreKeySpaces().contains(path.keyspace) || path.keyspace.equals(SYSTEM_KEYSPACE)))
            return;
        count.incrementAndGet();
        executor.submit(new RetryableCallable<Integer>()
        {
            @Override
            public Integer retriableCall() throws Exception
            {
                logger.info("Downloading file: " + path);
                fs.download(path, new FileOutputStream(restoreLocation));
                // TODO: fix me -> if there is exception the why hang?
                return count.decrementAndGet();
            }
        });
    }
    
    protected void waitToComplete()
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
}
