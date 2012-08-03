package com.netflix.priam.backup;

public class BackupRestoreException extends Exception
{

    private static final long serialVersionUID = 333L;

    public BackupRestoreException(String message)
    {
        super(message);
    }

    public BackupRestoreException(String message, Exception e)
    {
        super(message, e);
    }

}
