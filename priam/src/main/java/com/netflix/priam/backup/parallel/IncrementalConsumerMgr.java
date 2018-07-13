/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.backup.parallel;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IIncrementalBackup;
import com.netflix.priam.config.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/*
 * Monitors files to be uploaded and assigns each file to a worker
 */
public class IncrementalConsumerMgr implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(IncrementalConsumerMgr.class);

    private AtomicBoolean run = new AtomicBoolean(true);
    private ListeningExecutorService executor;
    private IBackupFileSystem fs;
    private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;
    private BackupPostProcessingCallback<AbstractBackupPath> callback;

    public IncrementalConsumerMgr(ITaskQueueMgr<AbstractBackupPath> taskQueueMgr, IBackupFileSystem fs
            , IConfiguration config
    ) {
        this.taskQueueMgr = taskQueueMgr;
        this.fs = fs;

		/*
         * Too few threads, the queue will build up, consuming a lot of memory.
		 * Too many threads on the other hand will slow down the whole system due to excessive context switches - and lead to same symptoms.
		 */
        int maxWorkers = config.getIncrementalBkupMaxConsumers();
        /*
		 * ThreadPoolExecutor will move the file to be uploaded as a Runnable task in the work queue.
		 */
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(config.getIncrementalBkupMaxConsumers() * 2);
		/*
		 * If there all workers are busy, the calling thread for the submit() will itself upload the file.  This is a way to throttle how many files are moved to the
		 * worker queue.  Specifically, the calling will continue to perform the upload unless a worker is avaialble.
		 */
        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        executor = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(maxWorkers, maxWorkers, 60, TimeUnit.SECONDS,
                                                                           workQueue, rejectedExecutionHandler));

        callback = new IncrementalBkupPostProcessing(this.taskQueueMgr);
    }

    /*
     * Stop looking for files to upload
     */
    public void shutdown() {
        this.run.set(false);
        this.executor.shutdown(); //will not accept new task and waits for active threads to be completed before shutdown.
    }

    @Override
    public void run() {
        while (this.run.get()) {

            while (this.taskQueueMgr.hasTasks()) {
                try {
                    final AbstractBackupPath bp = this.taskQueueMgr.take();

                    IncrementalConsumer task = new IncrementalConsumer(bp, this.fs, this.callback);
                    ListenableFuture<?> upload = executor.submit(task); //non-blocking, will be rejected if the task cannot be scheduled
                    Futures.addCallback(upload, new FutureCallback<Object>()
                    {
                        public void onSuccess(@Nullable Object result) { }

                        public void onFailure(Throwable t) {
                            // The post processing hook is responsible for removing the task from the de-duplicating
                            // HashSet, so we want to do the safe thing here and remove it just in case so the
                            // producers can re-enqueue this file in the next iteration.
                            // Note that this should be an abundance of caution as the IncrementalConsumer _should_
                            // have deleted the task from the queue when it internally failed.
                            taskQueueMgr.taskPostProcessing(bp);
                        }
                    });
                } catch (InterruptedException e) {
                    logger.warn("Was interrupted while wating to dequeued a task.  Msgl: {}", e.getLocalizedMessage());
                }
            }

            // Lets not overwhelm the node hence we will pause before checking the work queue again.
            try {
                Thread.sleep(IIncrementalBackup.INCREMENTAL_INTERVAL_IN_MILLISECS);
            } catch (InterruptedException e) {
                logger.warn("Was interrupted while sleeping until next interval run.  Msgl: {}", e.getLocalizedMessage());
            }
        }

    }

}