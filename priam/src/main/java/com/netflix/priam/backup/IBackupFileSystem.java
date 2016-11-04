/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
     * Write the contents of the specified remote path to the output stream and close.
     * filePath denotes the diskPath of the downloaded file
     */
    public void download(AbstractBackupPath path, OutputStream os, String filePath) throws BackupRestoreException;

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

    public long getBytesUploaded();
}
