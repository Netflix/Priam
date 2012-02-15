package com.netflix.priam;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;

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

    @Override
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException
    {
        // TODO Auto-generated method stub
        
    }
}