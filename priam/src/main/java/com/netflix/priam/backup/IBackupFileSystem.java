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

import com.google.common.util.concurrent.ListenableFuture;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/** Interface representing a backup storage as a file system */
public interface IBackupFileSystem {
    /**
     * Download the file denoted by remotePath to the local file system denoted by local path.
     *
     * @param path Backup path representing a local and remote file pair
     * @param retry No. of times to retry to download a file from remote file system. If &lt;1, it
     *     will try to download file exactly once.
     * @throws BackupRestoreException if file is not available, downloadable or any other error from
     *     remote file system.
     */
    void downloadFile(AbstractBackupPath path, String suffix, int retry)
            throws BackupRestoreException;

    /**
     * Download the file denoted by remotePath in an async fashion to the local file system denoted
     * by local path.
     *
     * @param path Backup path representing a local and remote file pair
     * @param retry No. of times to retry to download a file from remote file system. If &lt;1, it
     *     will try to download file exactly once.
     * @return The future of the async job to monitor the progress of the job.
     * @throws BackupRestoreException if file is not available, downloadable or any other error from
     *     remote file system.
     * @throws RejectedExecutionException if the queue is full and TIMEOUT is reached while trying
     *     to add the work to the queue.
     */
    Future<Path> asyncDownloadFile(final AbstractBackupPath path, final int retry)
            throws BackupRestoreException, RejectedExecutionException;

    /** Overload that uploads as fast as possible without any custom throttling */
    default void uploadAndDelete(AbstractBackupPath path, boolean async)
            throws FileNotFoundException, BackupRestoreException {
        uploadAndDelete(path, Instant.EPOCH, async);
    }

    /**
     * Upload the local file to its remote counterpart in an optionally async fashion. Both
     * locations are embedded within the path parameter. De-duping of the file to upload will always
     * be done by comparing the files-in-progress to be uploaded. This may result in this particular
     * request to not to be executed e.g. if any other thread has given the same file to upload and
     * that file is in internal queue. Note that de-duping is best effort and is not always
     * guaranteed as we try to avoid lock on read/write of the files-in-progress. Once uploaded,
     * files are deleted. Uploads are retried 10 times.
     *
     * @param path AbstractBackupPath to be used to send backup notifications only.
     * @param target The target time of completion of all files in the upload.
     * @param async boolean to determine whether the call should block or return immediately and
     *     upload asynchronously
     * @return The future of the async job to monitor the progress of the job. This will be null if
     *     file was de-duped for upload.
     * @throws BackupRestoreException in case of failure to upload for any reason including file not
     *     readable or remote file system errors.
     * @throws FileNotFoundException If a file as denoted by localPath is not available or is a
     *     directory.
     * @throws RejectedExecutionException if the queue is full and TIMEOUT is reached while trying
     *     to add the work to the queue.
     */
    ListenableFuture<AbstractBackupPath> uploadAndDelete(
            final AbstractBackupPath path, Instant target, boolean async)
            throws FileNotFoundException, RejectedExecutionException, BackupRestoreException;

    /**
     * Get the shard where this object should be stored. For local file system this should be empty
     * or null. For S3, it would be the location of the bucket.
     *
     * @return the location of the shard.
     */
    default String getShard() {
        return "";
    }

    /**
     * Get the prefix path for the backup file system. This will be either the location of the
     * remote file system for backup or the location from where we should restore.
     *
     * @return prefix path to the backup file system.
     */
    Path getPrefix();

    /**
     * List all files in the backup location for the specified time range.
     *
     * @param path This is used as the `prefix` for listing files in the filesystem. All the files
     *     that start with this prefix will be returned.
     * @param start Start date of the file upload.
     * @param till End date of the file upload.
     * @return Iterator of the AbstractBackupPath matching the criteria.
     */
    Iterator<AbstractBackupPath> list(String path, Date start, Date till);

    /** Get a list of prefixes for the cluster available in backup for the specified date */
    Iterator<AbstractBackupPath> listPrefixes(Date date);

    /**
     * List all the files with the given prefix, delimiter, and marker. Files should be returned
     * ordered by last modified time descending. This should never return null.
     *
     * @param prefix Common prefix of the elements to search in the backup file system.
     * @param delimiter All the object will end with this delimiter.
     * @param marker Start the fetch with this as the first object.
     * @return the iterator on the backup file system containing path of the files.
     */
    Iterator<String> listFileSystem(String prefix, String delimiter, String marker);

    /** Runs cleanup or set retention */
    void cleanup();

    /** Give the file system a chance to terminate any thread pools, etc. */
    void shutdown();

    /**
     * Get the size of the remote object
     *
     * @param remotePath Location of the object on the remote file system.
     * @return size of the object on the remote filesystem.
     * @throws BackupRestoreException in case of failure to read object denoted by remotePath or any
     *     other error.
     */
    long getFileSize(String remotePath) throws BackupRestoreException;

    /**
     * Checks if the file denoted by remotePath exists on the remote file system. It does not need
     * check if object was completely uploaded to remote file system.
     *
     * @param remotePath location on the remote file system.
     * @return boolean value indicating presence of the file on remote file system.
     */
    default boolean checkObjectExists(Path remotePath) {
        return false;
    }

    /**
     * Delete list of remote files from the remote file system. It should throw exception if there
     * is anything wrong in processing the request. If the remotePath passed do not exist, then it
     * should just keep quiet.
     *
     * @param remotePaths list of files on remote file system to be deleted. This path may or may
     *     not exist.
     * @throws BackupRestoreException in case of remote file system not able to process the request
     *     or unable to reach.
     */
    void deleteRemoteFiles(List<Path> remotePaths) throws BackupRestoreException;

    /**
     * Get the number of tasks en-queue in the filesystem for upload.
     *
     * @return the total no. of tasks to be executed.
     */
    int getUploadTasksQueued();

    /**
     * Get the number of tasks en-queue in the filesystem for download.
     *
     * @return the total no. of tasks to be executed.
     */
    int getDownloadTasksQueued();

    /** Clear the cache for the backup file system, if any. */
    void clearCache();
}
