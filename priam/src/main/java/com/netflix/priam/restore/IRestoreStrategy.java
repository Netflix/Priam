package com.netflix.priam.restore;

import java.util.Date;

/*
 * A means to restore C* files from various source types (e.g. Google, AWS bucket whose objects are not owned by the current IAM role), and encrypted / non-encrypted data.
 */
public interface IRestoreStrategy {
	public void restore(Date startTime, Date endTime) throws Exception;
}
