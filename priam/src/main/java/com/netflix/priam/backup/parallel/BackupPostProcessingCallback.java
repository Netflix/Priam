package com.netflix.priam.backup.parallel;

/*
 * Encapsules one to many steps needed once an upload is completed.
 */
public interface BackupPostProcessingCallback<E> {
	
	public void postProcessing(E completedTask);
}