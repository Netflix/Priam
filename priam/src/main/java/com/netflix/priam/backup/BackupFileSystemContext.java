package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;

public class BackupFileSystemContext implements IFileSystemContext {

	private IBackupFileSystem fs = null, encryptedFs = null;

	@Inject
	public BackupFileSystemContext(@Named("backup")IBackupFileSystem fs, @Named("encryptedbackup") IBackupFileSystem encryptedFs) {
		
		this.fs = fs;
		this.encryptedFs = encryptedFs;
	}
	
	public IBackupFileSystem getFileStrategy(IConfiguration config) {
		
		if (!config.isEncryptBackupEnabled()) {
			
			return this.fs;
			
		} else {

			return this.encryptedFs;
		}
	}
}