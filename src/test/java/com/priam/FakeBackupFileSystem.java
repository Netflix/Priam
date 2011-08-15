package com.priam;

import java.util.Date;
import java.util.Iterator;

import com.priam.backup.AbstractBackupPath;
import com.priam.backup.BackupRestoreException;
import com.priam.backup.IBackupFileSystem;

public class FakeBackupFileSystem implements IBackupFileSystem
{

    @Override
    public void download(AbstractBackupPath path) throws BackupRestoreException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void upload(AbstractBackupPath path) throws BackupRestoreException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getActivecount()
    {
        // TODO Auto-generated method stub
        return 0;
    }
}