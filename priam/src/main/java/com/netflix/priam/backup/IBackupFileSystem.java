package com.netflix.priam.backup;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

/**
 * Interface representing a backup storage as a file system
 */
public interface IBackupFileSystem
{
    /**
     * Write the contents of the specified remote path to the output stream and
     * close
     */
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException;

    /**
     * Upload/Backup to the specified location with contents from the input
     * stream. Closes the InputStream after its done.
     */
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException;

    /**
     * List all files in the backup location for the specified time range.
     */
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till);
    
    /**
     * Get a list of prefixes for the cluster available in backup for the specified date
     */
    public Iterator<AbstractBackupPath> listPrefixes(Date date);

    /**
     * Runs cleanup or set retention
     */
    public void cleanup();
    
    /**
     * Get number of active upload or downloads
     */
    public int getActivecount();

    /**
     * Give the file system a chance to terminate any thread pools, etc.
     */
    public void shutdown();
}
