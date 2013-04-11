/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.NamedThreadPoolExecutor;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.FifoQueue;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;

public abstract class AbstractRestore extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestore.class);
    private static final String SYSTEM_KEYSPACE = "system";
    // keeps track of the last few download which was executed.
    // TODO fix the magic number of 1000 => the idea of 80% of 1000 files limit per s3 query
    protected static final FifoQueue<AbstractBackupPath> tracker = new FifoQueue<AbstractBackupPath>(800);
    private AtomicInteger count = new AtomicInteger();
    protected final IBackupFileSystem fs;
    
    protected final IConfiguration config;
    protected final ThreadPoolExecutor executor;

    public static BigInteger restoreToken;
    
    protected final Sleeper sleeper;
    
    public AbstractRestore(IConfiguration config, IBackupFileSystem fs, String name, Sleeper sleeper)
    {
        super(config);
        this.config = config;
        this.fs = fs;
        this.sleeper = sleeper;
        executor = new NamedThreadPoolExecutor(config.getMaxBackupDownloadThreads(), name);
        executor.allowCoreThreadTimeOut(true);
    }

    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter) throws Exception
    {
        while (fsIterator.hasNext())
        {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.type == BackupFileType.SST && tracker.contains(temp))
                continue;
            if (temp.getType() == filter)
                download(temp, temp.newRestoreFile());
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
                logger.info("Downloading file: " + path + " to: " + restoreLocation);
                fs.download(path, new FileOutputStream(restoreLocation),restoreLocation.getAbsolutePath());
                tracker.adjustAndAdd(path);
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
                sleeper.sleep(1000);
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }
    
    protected AtomicInteger getFileCount()
    {
    		return count;
    }
    
    protected void setFileCount(int cnt)
    {
    		count.set(cnt);
    }
}
