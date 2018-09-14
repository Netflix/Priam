/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.restore;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.MetaData;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.NamedThreadPoolExecutor;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main class for restoring data from backup. Backup restored using this way are not encrypted.
 */
@Singleton
public class Restore extends AbstractRestore {
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    private final ThreadPoolExecutor executor;
    private AtomicInteger count = new AtomicInteger();

    @Inject
    public Restore(IConfiguration config, @Named("backup") IBackupFileSystem fs, Sleeper sleeper, ICassandraProcess cassProcess,
                   Provider<AbstractBackupPath> pathProvider,
                   InstanceIdentity instanceIdentity, RestoreTokenSelector tokenSelector, MetaData metaData, InstanceState instanceState, IPostRestoreHook postRestoreHook) {
        super(config, fs, JOBNAME, sleeper, pathProvider, instanceIdentity, tokenSelector, cassProcess, metaData, instanceState, postRestoreHook);
        executor = new NamedThreadPoolExecutor(config.getMaxBackupDownloadThreads(), JOBNAME);
        executor.allowCoreThreadTimeOut(true);
    }

    @Override
    protected final void downloadFile(final AbstractBackupPath path, final File restoreLocation) throws Exception {
        count.incrementAndGet();
        executor.submit(new RetryableCallable<Integer>() {
            @Override
            public Integer retriableCall() throws Exception {
                fs.downloadFile(Paths.get(path.getRemotePath()), Paths.get(restoreLocation.getAbsolutePath()));
                tracker.adjustAndAdd(path);
                // TODO: fix me -> if there is exception the why hang?
                return count.decrementAndGet();
            }
        });
    }

    @Override
    protected final void waitToComplete() {
        while (count.get() != 0) {
            try {
                sleeper.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public int getActiveCount() {
        return (executor == null) ? 0 : executor.getActiveCount();
    }
}