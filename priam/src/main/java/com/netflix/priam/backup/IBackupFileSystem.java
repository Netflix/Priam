/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.backup;

import java.nio.file.Path;
import java.util.Date;
import java.util.Iterator;

/**
 * Interface representing a backup storage as a file system
 */
public interface IBackupFileSystem {
    /**
     * Download the file denoted by remotePath to the local file system denoted by local path.
     *
     * @param remotePath fully qualified location of the file on remote file system.
     * @param localPath  location on the local file sytem where remote file should be downloaded.
     * @throws BackupRestoreException if file is not available, downloadable or any other error from remote file system.
     */
    void downloadFile(Path remotePath, Path localPath) throws BackupRestoreException;

    /**
     * Upload the local file denoted by localPath to the remote file system at location denoted by remotePath.
     *
     * @param localPath  Path of the local file that needs to be uploaded.
     * @param remotePath Fully qualified path on the remote file system where file should be uploaded.
     * @param path       AbstractBackupPath to be used to send backup notifications only.
     * @throws BackupRestoreException in case of failure to upload for any reason including file not available, readable or remote file system errors.
     */
    void uploadFile(Path localPath, Path remotePath, AbstractBackupPath path) throws BackupRestoreException;

    /**
     * List all files in the backup location for the specified time range.
     */
    Iterator<AbstractBackupPath> list(String path, Date start, Date till);

    /**
     * Get a list of prefixes for the cluster available in backup for the specified date
     */
    Iterator<AbstractBackupPath> listPrefixes(Date date);

    /**
     * Runs cleanup or set retention
     */
    void cleanup();

    /**
     * Give the file system a chance to terminate any thread pools, etc.
     */
    void shutdown();

    /**
     * Get the size of the remote object
     *
     * @param remotePath Location of the object on the remote file system.
     * @return size of the object on the remote filesystem.
     * @throws BackupRestoreException in case of failure to read object denoted by remotePath or any other error.
     */
    long getFileSize(Path remotePath) throws BackupRestoreException;

}
