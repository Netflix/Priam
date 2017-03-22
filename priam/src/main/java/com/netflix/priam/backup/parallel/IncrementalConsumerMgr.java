/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.backup.parallel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.priam.notification.BackupNotificationMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IIncrementalBackup;

/*
 * Monitors files to be uploaded and assigns each file to a worker
 */
public class IncrementalConsumerMgr implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(IncrementalConsumerMgr.class);
	
	private AtomicBoolean run = new AtomicBoolean(true);
	private ThreadPoolExecutor executor;
	private IBackupFileSystem fs;
	private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;
	private BackupPostProcessingCallback<AbstractBackupPath> callback;
	private BackupNotificationMgr backupNotificationMgr;

	
	public IncrementalConsumerMgr(ITaskQueueMgr<AbstractBackupPath> taskQueueMgr, IBackupFileSystem fs
			, IConfiguration config
			, BackupNotificationMgr backupNotificationMgr
		) {
		this.taskQueueMgr = taskQueueMgr;
		this.fs = fs;
		this.backupNotificationMgr = backupNotificationMgr;
		
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
		executor = new ThreadPoolExecutor(maxWorkers, maxWorkers, 60, TimeUnit.SECONDS,
                workQueue, rejectedExecutionHandler);
		
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
		while(this.run.get()) {

			while( this.taskQueueMgr.hasTasks() ) {
				try {
					AbstractBackupPath bp = this.taskQueueMgr.take();
					
					IncrementalConsumer task = new IncrementalConsumer(bp, this.fs, this.callback, this.backupNotificationMgr);
					executor.submit(task); //non-blocking, will be rejected if the task cannot be scheduled

					
				} catch (InterruptedException e) {
					logger.warn("Was interrupted while wating to dequeued a task.  Msgl: " + e.getLocalizedMessage());
				}
			}
			
			//Lets not overwhelmend the node hence we will pause before checking the work queue again.
			try {
				Thread.currentThread().sleep(IIncrementalBackup.INCREMENTAL_INTERVAL_IN_MILLISECS);
			} catch (InterruptedException e) {
				logger.warn("Was interrupted while sleeping until next interval run.  Msgl: " + e.getLocalizedMessage());
			}
		}
		
	}

}