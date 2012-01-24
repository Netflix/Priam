package com.priam.backup;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

public interface IBackupFileSystem
{
    public void download(AbstractBackupPath path) throws BackupRestoreException;

    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException;

    public void upload(AbstractBackupPath path) throws BackupRestoreException;

    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException;

    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till);

    public int getActivecount();
}
