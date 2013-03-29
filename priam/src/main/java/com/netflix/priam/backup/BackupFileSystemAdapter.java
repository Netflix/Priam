package com.netflix.priam.backup;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

public abstract class BackupFileSystemAdapter implements IBackupFileSystem{

    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException {
	}

    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException {
	}

    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
		return null;
	}
    
    public Iterator<AbstractBackupPath> listPrefixes(Date date) {
		return null;
	}

    public void cleanup() {
	}
    
    public int getActivecount() {
		return 0;
	}

    public void shutdown() {
	}
}
