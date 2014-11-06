package com.netflix.priam.backup;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

import com.google.inject.Singleton;

@Singleton
public class FakedS3EncryptedFileSystem implements IBackupFileSystem {

	@Override
	public void download(AbstractBackupPath path, OutputStream os)
			throws BackupRestoreException {
		// TODO Auto-generated method stub

	}

	@Override
	public void download(AbstractBackupPath path, OutputStream os,
			String filePath) throws BackupRestoreException {
		// TODO Auto-generated method stub

	}

	@Override
	public void upload(AbstractBackupPath path, InputStream in)
			throws BackupRestoreException {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<AbstractBackupPath> listPrefixes(Date date) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public int getActivecount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

}
