package com.netflix.priam.backup;

import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;

@Singleton
public class FakedBackupFileSystemContext implements IFileSystemContext {

	@Override
	public IBackupFileSystem getFileStrategy(IConfiguration config) {
		// TODO Auto-generated method stub
		return null;
	}

}
