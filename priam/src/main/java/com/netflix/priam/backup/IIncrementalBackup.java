package com.netflix.priam.backup;

import com.netflix.priam.scheduler.TaskTimer;

public interface IIncrementalBackup {
	
	public static long INCREMENTAL_INTERVAL_IN_MILLISECS = 10L * 1000;
	
	/*
	 * @return the number of files pending to be uploaded.  The semantic depends on whether the implementation
	 * is synchronous or asynchronous.
	 */
	public long getNumPendingFiles();
	
	public String getJobName();

}