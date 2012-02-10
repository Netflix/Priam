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
     * Download/Restore the specified remote file
     * 
     * @param path
     * @throws BackupRestoreException
     */
    public void download(AbstractBackupPath path) throws BackupRestoreException;

    /**
     * Write the contents of the specified remote path to the output stream and
     * close
     * 
     * @param path
     * @param os
     * @throws BackupRestoreException
     */
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException;

    /**
     * Upload/Backup the specified path
     * 
     * @param path
     * @throws BackupRestoreException
     */
    public void upload(AbstractBackupPath path) throws BackupRestoreException;

    /**
     * Upload/Backup to the specified location with contents from the input
     * stream. Closes the InputStream after its done.
     * 
     * @param path
     * @param in
     * @throws BackupRestoreException
     */
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException;

    /**
     * List all files in the backup location for the specified time range.
     * 
     * @param bucket
     * @param start
     * @param till
     * @return
     */
    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till);

    /**
     * Get number of active upload or downloads
     * 
     * @return
     */
    public int getActivecount();
}
