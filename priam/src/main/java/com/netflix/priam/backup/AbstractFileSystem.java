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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupEvent;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.EventGenerator;
import com.netflix.priam.notification.EventObserver;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import com.netflix.spectator.api.patterns.PolledMeter;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
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
public abstract class AbstractFileSystem implements IBackupFileSystem, EventGenerator<BackupEvent> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFileSystem.class);
    protected final Provider<AbstractBackupPath> pathProvider;
    private final CopyOnWriteArrayList<EventObserver<BackupEvent>> observers =
            new CopyOnWriteArrayList<>();
    private final IConfiguration configuration;
    protected final BackupMetrics backupMetrics;
    private final Set<Path> tasksQueued;
    private final ThreadPoolExecutor fileUploadExecutor;
    private final ThreadPoolExecutor fileDownloadExecutor;

    @Inject
    public AbstractFileSystem(
            IConfiguration configuration,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationMgr,
            Provider<AbstractBackupPath> pathProvider) {
        this.configuration = configuration;
        this.backupMetrics = backupMetrics;
        this.pathProvider = pathProvider;
        // Add notifications.
        this.addObserver(backupNotificationMgr);
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
                new BlockingSubmitThreadPoolExecutor(
                        configuration.getBackupThreads(),
                        uploadQueue,
                        configuration.getUploadTimeout());

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
    public Future<Path> asyncDownloadFile(
            final Path remotePath, final Path localPath, final int retry)
            throws BackupRestoreException, RejectedExecutionException {
        return fileDownloadExecutor.submit(
                () -> {
                    downloadFile(remotePath, localPath, retry);
                    return remotePath;
                });
    }

    @Override
    public void downloadFile(final Path remotePath, final Path localPath, final int retry)
            throws BackupRestoreException {
        // TODO: Should we download the file if localPath already exists?
        if (remotePath == null || localPath == null) return;
        localPath.toFile().getParentFile().mkdirs();
        logger.info("Downloading file: {} to location: {}", remotePath, localPath);
        try {
            new BoundedExponentialRetryCallable<Void>(500, 10000, retry) {
                @Override
                public Void retriableCall() throws Exception {
                    downloadFileImpl(remotePath, localPath);
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

    protected abstract void downloadFileImpl(final Path remotePath, final Path localPath)
            throws BackupRestoreException;

    @Override
    public Future<Path> asyncUploadFile(
            final Path localPath,
            final Path remotePath,
            final AbstractBackupPath path,
            final int retry,
            final boolean deleteAfterSuccessfulUpload)
            throws FileNotFoundException, RejectedExecutionException, BackupRestoreException {
        return fileUploadExecutor.submit(
                () -> {
                    uploadFile(localPath, remotePath, path, retry, deleteAfterSuccessfulUpload);
                    return localPath;
                });
    }

    @Override
    public void uploadFile(
            final Path localPath,
            final Path remotePath,
            final AbstractBackupPath path,
            final int retry,
            final boolean deleteAfterSuccessfulUpload)
            throws FileNotFoundException, BackupRestoreException {
        if (localPath == null
                || remotePath == null
                || !localPath.toFile().exists()
                || localPath.toFile().isDirectory())
            throw new FileNotFoundException(
                    "File do not exist or is a directory. localPath: "
                            + localPath
                            + ", remotePath: "
                            + remotePath);

        if (tasksQueued.add(localPath)) {
            logger.info("Uploading file: {} to location: {}", localPath, remotePath);
            try {
                notifyEventStart(new BackupEvent(path));
                long uploadedFileSize;

                // Upload file if it not present at remote location.
                if (path.getType() != BackupFileType.SST_V2 || !doesRemoteFileExist(remotePath)) {
                    uploadedFileSize =
                            new BoundedExponentialRetryCallable<Long>(500, 10000, retry) {
                                @Override
                                public Long retriableCall() throws Exception {
                                    return uploadFileImpl(localPath, remotePath);
                                }
                            }.call();
                    backupMetrics.recordUploadRate(uploadedFileSize);
                    backupMetrics.incrementValidUploads();
                } else {
                    // file is already uploaded to remote file system. get the compressed file size
                    // to update it for notification message.
                    uploadedFileSize = getFileSize(remotePath);
                    logger.info(
                            "File: {} already present on remoteFileSystem with file size: {}",
                            remotePath,
                            uploadedFileSize);
                }

                path.setCompressedFileSize(uploadedFileSize);
                notifyEventSuccess(new BackupEvent(path));
                logger.info(
                        "Successfully uploaded file: {} to location: {}", localPath, remotePath);

                if (deleteAfterSuccessfulUpload && !FileUtils.deleteQuietly(localPath.toFile()))
                    logger.warn(
                            String.format(
                                    "Failed to delete local file %s.",
                                    localPath.toFile().getAbsolutePath()));

            } catch (Exception e) {
                backupMetrics.incrementInvalidUploads();
                notifyEventFailure(new BackupEvent(path));
                logger.error(
                        "Error while uploading file: {} to location: {}", localPath, remotePath);
                throw new BackupRestoreException(e.getMessage());
            } finally {
                // Remove the task from the list so if we try to upload file ever again, we can.
                tasksQueued.remove(localPath);
            }
        } else logger.info("Already in queue, no-op.  File: {}", localPath);
    }

    protected abstract long uploadFileImpl(final Path localPath, final Path remotePath)
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
    public final void addObserver(EventObserver<BackupEvent> observer) {
        if (observer == null) throw new NullPointerException("observer must not be null.");

        observers.addIfAbsent(observer);
    }

    @Override
    public void removeObserver(EventObserver<BackupEvent> observer) {
        if (observer == null) throw new NullPointerException("observer must not be null.");

        observers.remove(observer);
    }

    @Override
    public void notifyEventStart(BackupEvent event) {
        observers.forEach(eventObserver -> eventObserver.updateEventStart(event));
    }

    @Override
    public void notifyEventSuccess(BackupEvent event) {
        observers.forEach(eventObserver -> eventObserver.updateEventSuccess(event));
    }

    @Override
    public void notifyEventFailure(BackupEvent event) {
        observers.forEach(eventObserver -> eventObserver.updateEventFailure(event));
    }

    @Override
    public void notifyEventStop(BackupEvent event) {
        observers.forEach(eventObserver -> eventObserver.updateEventStop(event));
    }

    @Override
    public int getUploadTasksQueued() {
        return tasksQueued.size();
    }

    @Override
    public int getDownloadTasksQueued() {
        return fileDownloadExecutor.getQueue().size();
    }
}
