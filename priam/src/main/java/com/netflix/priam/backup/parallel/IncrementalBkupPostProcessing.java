package com.netflix.priam.backup.parallel;

import com.netflix.priam.backup.AbstractBackupPath;

public class IncrementalBkupPostProcessing implements BackupPostProcessingCallback<AbstractBackupPath> {

	private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;

	public IncrementalBkupPostProcessing(ITaskQueueMgr<AbstractBackupPath> taskQueueMgr) {
		this.taskQueueMgr = taskQueueMgr;
	}
	
	@Override
	public void postProcessing(AbstractBackupPath completedTask) {
		this.taskQueueMgr.taskPostProcessing(completedTask);
	}

}