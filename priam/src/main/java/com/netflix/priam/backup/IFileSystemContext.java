package com.netflix.priam.backup;

import com.netflix.priam.IConfiguration;

public interface IFileSystemContext {
	public IBackupFileSystem getFileStrategy(IConfiguration config);
}
