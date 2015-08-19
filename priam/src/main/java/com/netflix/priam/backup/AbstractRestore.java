/**
 * Copyright 2013 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.NamedThreadPoolExecutor;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.FifoQueue;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;

public abstract class AbstractRestore extends Task {
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

    public AbstractRestore(IConfiguration config, IBackupFileSystem fs, String name, Sleeper sleeper) {
        super(config);
        this.config = config;
        this.fs = fs;
        this.sleeper = sleeper;
        executor = new NamedThreadPoolExecutor(config.getMaxBackupDownloadThreads(), name);
        executor.allowCoreThreadTimeOut(true);
    }

    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter) throws Exception {
        List<Future<?>> futures = Lists.newArrayList();

        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.type == BackupFileType.SST && tracker.contains(temp))
                continue;

            if (temp.getType() == filter) {
                File localFileHandler = temp.newRestoreFile();
                logger.debug("Created local file name: %s", localFileHandler.getAbsolutePath() + File.pathSeparator + localFileHandler.getName());
                futures.add(download(temp, localFileHandler));
            }
        }
        waitToComplete(futures);
    }

    private class BoundedList<E> extends LinkedList<E> {

        private final int limit;

        public BoundedList(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean add(E o) {
            super.add(o);
            while (size() > limit) {
                super.remove();
            }
            return true;
        }
    }

    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter, int lastN) throws Exception {
        if (fsIterator == null)
            return;

        BoundedList bl = new BoundedList(lastN);
        while (fsIterator.hasNext()) {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.type == BackupFileType.SST && tracker.contains(temp))
                continue;

            if (temp.getType() == filter) {
                bl.add(temp);
            }
        }

        download(bl.iterator(), filter);
    }

    /**
     * Download to specific location
     */
    public Future<?> download(final AbstractBackupPath path, final File restoreLocation) throws Exception {
        if (config.getRestoreKeySpaces().size() != 0 && (!config.getRestoreKeySpaces().contains(path.keyspace) || path.keyspace.equals(SYSTEM_KEYSPACE)))
            return Futures.immediateFuture(count.get());
        count.incrementAndGet();
        return executor.submit(new RetryableCallable<Integer>() {
            @Override
            public Integer retriableCall() throws Exception {
                logger.info("Downloading file: " + path + " to: " + restoreLocation);
                fs.download(path, new FileOutputStream(restoreLocation), restoreLocation.getAbsolutePath());
                tracker.adjustAndAdd(path);
                return count.decrementAndGet();
            }
        });
    }

    protected void waitToComplete(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            } catch (ExecutionException e) {
                logger.error("Wait for future task error: {}", e.getCause());
            }
        }
    }

    protected AtomicInteger getFileCount() {
        return count;
    }

    protected void setFileCount(int cnt) {
        count.set(cnt);
    }
}
