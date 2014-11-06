package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;

@Singleton
public class FakedBackupFileSystemContext implements IFileSystemContext {

	private IBackupFileSystem fs;

	@Inject
	public FakedBackupFileSystemContext(@Named("backup") IBackupFileSystem fs) {
		this.fs = fs;
	}
	
	@Override
	public IBackupFileSystem getFileStrategy(IConfiguration config) {
		return this.fs;
	}

}
