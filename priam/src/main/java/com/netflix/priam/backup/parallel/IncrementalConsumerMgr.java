package com.netflix.priam.backup.parallel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IIncrementalBackup;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;

/*
 * Monitors files to be uploaded and assigns each file to a worker
 */
public class IncrementalConsumerMgr implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(IncrementalConsumerMgr.class);
	
	private AtomicBoolean run = new AtomicBoolean(true);
	private ThreadPoolExecutor executor;
	private IBackupFileSystem fs;
	private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;
	BackupPostProcessingCallback<AbstractBackupPath> callback;
	private IConfiguration config;

	
	public IncrementalConsumerMgr(IConfiguration config, ITaskQueueMgr<AbstractBackupPath> taskQueueMgr, IBackupFileSystem fs) {
		this.taskQueueMgr = taskQueueMgr;
		this.fs = fs;
		this.config = config;
		
		/*
		 * Too few threads, the queue will build up, consuming a lot of memory.
		 * Too many threads on the other hand will slow down the whole system due to excessive context switches - and lead to same symptoms.
		 */
		int maxWorkers = this.config.getIncrementalBkupMaxConsumers();
		//number of tasks that can be submitted and enqueued for later execution.  If the work queue is full, the submitted task will be "rejected" 
		BlockingQueue<Runnable> runnableQueue = new ArrayBlockingQueue<Runnable>(maxWorkers * 2);
		/*
		 * We have upper bound on maximum threads and work queue.  New task submitted will be rejected by ThreadPoolExecutor when these resources are
		 * saturated.  In this case, the thread submitting the task itself will run the task.  This provides a simple control mechanism that will slow down the rate
		 * that new tasks are submitted (i.e. you can't submit new task if you are busy yourself executing the task at which point there will 
		 * be free threads or the process repeats).
		 */
		 RejectedExecutionHandler rejecteTaskHandler = new ThreadPoolExecutor.CallerRunsPolicy();
		executor = new ThreadPoolExecutor(maxWorkers, maxWorkers, 60, TimeUnit.SECONDS,
				runnableQueue, rejecteTaskHandler); 
		
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
			
			logger.info("Number of files in queue: " + this.taskQueueMgr.getNumOfTasksToBeProessed());
			
			while( this.taskQueueMgr.hasTasks() ) {
				try {
					AbstractBackupPath bp = this.taskQueueMgr.take();
					logger.info("Dequeued task and adding to work queue (yet another queue!): " + bp.getFileName()
							+ ", approximate number of active threads uploading files" + executor.getActiveCount());
					IncrementalConsumer task = new IncrementalConsumer(bp, this.fs, this.callback);
					/*
					 * We have upper bound on maximum threads and work queue.  If  these resources are saturated, the task is "rejected".
					 * Our thread pool executor has a handler for rejected tasks that runs the rejected task directly in the 
					 * calling thread of the submit method, unless the executor has been shut down, in which case the task is discarded.   
					 * This provides a simple control mechanism that will slow down the rate
					 * that new tasks are submitted (i.e. you can't submit new task if you are busy yourself executing the task at which point there will 
					 * be free threads or the process repeats).
					 */
					executor.submit(task);

					
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