package com.priam.backup;

import java.util.Date;
import java.util.Iterator;

public interface IBackupFileSystem
{
    public void download(AbstractBackupPath path) throws BackupRestoreException;

    public void upload(AbstractBackupPath path) throws BackupRestoreException;

    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till);

    public int getActivecount();
}
