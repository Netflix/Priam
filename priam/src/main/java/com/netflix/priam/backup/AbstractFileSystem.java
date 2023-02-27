/*
 * Copyright 2018 Netflix, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.UploadStatus;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import com.netflix.spectator.api.patterns.PolledMeter;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.commons.collections4.iterators.FilterIterator;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for managing parallelism and orchestrating the upload and download, but
 * the subclasses actually implement the details of uploading a file.
 *
 * <p>Created by aagrawal on 8/30/18.
 */
public abstract class AbstractFileSystem implements IBackupFileSystem {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFileSystem.class);
    protected final Provider<AbstractBackupPath> pathProvider;
    private final IConfiguration configuration;
    protected final BackupMetrics backupMetrics;
    private final Set<Path> tasksQueued;
    private final ListeningExecutorService fileUploadExecutor;
    private final ThreadPoolExecutor fileDownloadExecutor;
    private final BackupNotificationMgr backupNotificationMgr;

    // This is going to be a write-thru cache containing the most frequently used items from remote
    // file system. This is to ensure that we don't make too many API calls to remote file system.
    private final Cache<Path, Boolean> objectCache;

    @Inject
    public AbstractFileSystem(
            IConfiguration configuration,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            Provider<AbstractBackupPath> pathProvider) {
        this.configuration = configuration;
        this.backupMetrics = backupMetrics;
        this.pathProvider = pathProvider;
        this.backupNotificationMgr = backupNotificationMgr;
        this.objectCache =
                CacheBuilder.newBuilder().maximumSize(configuration.getBackupQueueSize()).build();
        tasksQueued = new ConcurrentHashMap<>().newKeySet();
        /*
        Note: We are using different queue for upload and download as with Backup V2.0 we might download all the meta
        files for "sync" feature which might compete with backups for scheduling.
        Also, we may want to have different TIMEOUT for each kind of operation (upload/download) based on our file system choices.
        */
        BlockingQueue<Runnable> uploadQueue =
                new ArrayBlockingQueue<>(configuration.getBackupQueueSize());
        PolledMeter.using(backupMetrics.getRegistry())
                .withName(backupMetrics.uploadQueueSize)
                .monitorSize(uploadQueue);
        this.fileUploadExecutor =
                MoreExecutors.listeningDecorator(
                        new BlockingSubmitThreadPoolExecutor(
                                configuration.getBackupThreads(),
                                uploadQueue,
                                configuration.getUploadTimeout()));

        BlockingQueue<Runnable> downloadQueue =
                new ArrayBlockingQueue<>(configuration.getDownloadQueueSize());
        PolledMeter.using(backupMetrics.getRegistry())
                .withName(backupMetrics.downloadQueueSize)
                .monitorSize(downloadQueue);
        this.fileDownloadExecutor =
                new BlockingSubmitThreadPoolExecutor(
                        configuration.getRestoreThreads(),
                        downloadQueue,
                        configuration.getDownloadTimeout());
    }

    @Override
    public Future<Path> asyncDownloadFile(final AbstractBackupPath path, final int retry)
            throws RejectedExecutionException {
        return fileDownloadExecutor.submit(
                () -> {
                    downloadFile(path, "" /* suffix */, retry);
                    return Paths.get(path.getRemotePath());
                });
    }

    @Override
    public void downloadFile(final AbstractBackupPath path, String suffix, final int retry)
            throws BackupRestoreException {
        // TODO: Should we download the file if localPath already exists?
        String remotePath = path.getRemotePath();
        String localPath = path.newRestoreFile().getAbsolutePath() + suffix;
        logger.info("Downloading file: {} to location: {}", path.getRemotePath(), localPath);
        try {
            new BoundedExponentialRetryCallable<Void>(500, 10000, retry) {
                @Override
                public Void retriableCall() throws Exception {
                    downloadFileImpl(path, suffix);
                    return null;
                }
            }.call();
            // Note we only downloaded the bytes which are represented on file system (they are
            // compressed and maybe encrypted).
            // File size after decompression or decryption might be more/less.
            backupMetrics.recordDownloadRate(getFileSize(remotePath));
            backupMetrics.incrementValidDownloads();
            logger.info("Successfully downloaded file: {} to location: {}", remotePath, localPath);
        } catch (Exception e) {
            backupMetrics.incrementInvalidDownloads();
            logger.error("Error while downloading file: {} to location: {}", remotePath, localPath);
            throw new BackupRestoreException(e.getMessage());
        }
    }

    protected abstract void downloadFileImpl(final AbstractBackupPath path, String suffix)
            throws BackupRestoreException;

    @Override
    public ListenableFuture<AbstractBackupPath> uploadAndDelete(
            final AbstractBackupPath path, Instant target, boolean async)
            throws RejectedExecutionException, BackupRestoreException {
        if (async) {
            return fileUploadExecutor.submit(
                    () -> uploadAndDeleteInternal(path, target, 10 /* retries */));
        } else {
            return Futures.immediateFuture(uploadAndDeleteInternal(path, target, 10 /* retries */));
        }
    }

    @VisibleForTesting
    public AbstractBackupPath uploadAndDeleteInternal(
            final AbstractBackupPath path, Instant target, int retry)
            throws RejectedExecutionException, BackupRestoreException {
        Path localPath = Paths.get(path.getBackupFile().getAbsolutePath());
        File localFile = localPath.toFile();
        Preconditions.checkArgument(
                localFile.exists(), String.format("Can't upload nonexistent %s", localPath));
        Preconditions.checkArgument(
                !localFile.isDirectory(),
                String.format("Can only upload files %s is a directory", localPath));
        Path remotePath = Paths.get(path.getRemotePath());

        if (tasksQueued.add(localPath)) {
            logger.info("Uploading file: {} to location: {}", localPath, remotePath);
            try {
                long uploadedFileSize;

                // Upload file if it not present at remote location.
                if (path.getType() != BackupFileType.SST_V2 || !checkObjectExists(remotePath)) {
                    backupNotificationMgr.notify(path, UploadStatus.STARTED);
                    uploadedFileSize =
                            new BoundedExponentialRetryCallable<Long>(
                                    500 /* minSleep */, 10000 /* maxSleep */, retry) {
                                @Override
                                public Long retriableCall() throws Exception {
                                    return uploadFileImpl(path, target);
                                }
                            }.call();

                    // Add to cache after successful upload.
                    // We only add SST_V2 as other file types are usually not checked, so no point
                    // evicting our SST_V2 results.
                    if (path.getType() == BackupFileType.SST_V2) addObjectCache(remotePath);

                    backupMetrics.recordUploadRate(uploadedFileSize);
                    backupMetrics.incrementValidUploads();
                    path.setCompressedFileSize(uploadedFileSize);
                    backupNotificationMgr.notify(path, UploadStatus.SUCCESS);
                } else {
                    // file is already uploaded to remote file system.
                    logger.info("File: {} already present on remoteFileSystem.", remotePath);
                }

                logger.info(
                        "Successfully uploaded file: {} to location: {}", localPath, remotePath);

                if (!FileUtils.deleteQuietly(localFile))
                    logger.warn(
                            String.format(
                                    "Failed to delete local file %s.",
                                    localFile.getAbsolutePath()));

            } catch (Exception e) {
                backupMetrics.incrementInvalidUploads();
                logger.error(
                        "Error while uploading file: {} to location: {}. Exception: Msg: [{}], Trace: {}",
                        localPath,
                        remotePath,
                        e.getMessage(),
                        e);
                backupNotificationMgr.notify(path, UploadStatus.FAILED);
                throw new BackupRestoreException(e.getMessage());
            } finally {
                // Remove the task from the list so if we try to upload file ever again, we can.
                tasksQueued.remove(localPath);
            }
        } else logger.info("Already in queue, no-op.  File: {}", localPath);
        return path;
    }

    private void addObjectCache(Path remotePath) {
        objectCache.put(remotePath, Boolean.TRUE);
    }

    @Override
    public boolean checkObjectExists(Path remotePath) {
        // Check in cache, if remote file exists.
        Boolean cacheResult = objectCache.getIfPresent(remotePath);

        // Cache hit. Return the value.
        if (cacheResult != null) return cacheResult;

        // Cache miss - Check remote file system if object exist.
        boolean remoteFileExist = doesRemoteFileExist(remotePath);

        if (remoteFileExist) addObjectCache(remotePath);

        return remoteFileExist;
    }

    @Override
    public void deleteRemoteFiles(List<Path> remotePaths) throws BackupRestoreException {
        if (remotePaths == null) return;

        // Note that we are trying to implement write-thru cache here so it is good idea to
        // invalidate the cache first. This is important so that if there is any issue (because file
        // was deleted), it is caught by our snapshot job we can re-upload the file. This will also
        // help in ensuring that our validation job fails if there are any error caused due to TTL
        // of a file.
        objectCache.invalidateAll(remotePaths);
        deleteFiles(remotePaths);
    }

    protected abstract void deleteFiles(List<Path> remotePaths) throws BackupRestoreException;

    protected abstract boolean doesRemoteFileExist(Path remotePath);

    protected abstract long uploadFileImpl(final AbstractBackupPath path, Instant target)
            throws BackupRestoreException;

    @Override
    public String getShard() {
        return getPrefix().getName(0).toString();
    }

    @Override
    public Path getPrefix() {
        Path prefix = Paths.get(configuration.getBackupPrefix());

        if (StringUtils.isNotBlank(configuration.getRestorePrefix())) {
            prefix = Paths.get(configuration.getRestorePrefix());
        }

        return prefix;
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date) {
        String prefix = pathProvider.get().clusterPrefix(getPrefix().toString());
        Iterator<String> fileIterator = listFileSystem(prefix, File.pathSeparator, null);

        //noinspection unchecked
        return new TransformIterator(
                fileIterator,
                remotePath -> {
                    AbstractBackupPath abstractBackupPath = pathProvider.get();
                    abstractBackupPath.parsePartialPrefix(remotePath.toString());
                    return abstractBackupPath;
                });
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
        String prefix = pathProvider.get().remotePrefix(start, till, path);
        Iterator<String> fileIterator = listFileSystem(prefix, null, null);

        @SuppressWarnings("unchecked")
        TransformIterator<String, AbstractBackupPath> transformIterator =
                new TransformIterator(
                        fileIterator,
                        remotePath -> {
                            AbstractBackupPath abstractBackupPath = pathProvider.get();
                            abstractBackupPath.parseRemote(remotePath.toString());
                            return abstractBackupPath;
                        });

        return new FilterIterator<>(
                transformIterator,
                abstractBackupPath ->
                        (abstractBackupPath.getTime().after(start)
                                        && abstractBackupPath.getTime().before(till))
                                || abstractBackupPath.getTime().equals(start));
    }

    @Override
    public int getUploadTasksQueued() {
        return tasksQueued.size();
    }

    @Override
    public int getDownloadTasksQueued() {
        return fileDownloadExecutor.getQueue().size();
    }

    @Override
    public void clearCache() {
        objectCache.invalidateAll();
    }
}
