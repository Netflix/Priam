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
import com.netflix.priam.backupv2.FileUploadResult;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupEvent;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.EventGenerator;
import com.netflix.priam.notification.EventObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by aagrawal on 8/30/18.
 */
public abstract class AbstractFileSystem implements IBackupFileSystem, EventGenerator<BackupEvent> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractFileSystem.class);
    private final CopyOnWriteArrayList<EventObserver<BackupEvent>> observers = new CopyOnWriteArrayList<>();
    protected BackupMetrics backupMetrics;

    @Inject
    public AbstractFileSystem(BackupMetrics backupMetrics,
                              BackupNotificationMgr backupNotificationMgr){
        this.backupMetrics = backupMetrics;
        //Add notifications.
        this.addObserver(backupNotificationMgr);
    }

    @Override
    public void downloadFile(Path remotePath, Path localPath) throws BackupRestoreException{
        logger.info("Downloading file: {} to location: {}", remotePath, localPath);
        try{
            //TODO: Retries should ideally go here.
            downloadFileImpl(remotePath, localPath);
            backupMetrics.recordDownloadRate(getFileSize(remotePath));
            logger.info("Successfully downloaded file: {} to location: {}", remotePath, localPath);
        }catch (BackupRestoreException e){
            backupMetrics.incrementInvalidDownloads();
            logger.error("Error while downloading file: {} to location: {}", remotePath, localPath);
            throw e;
        }
    }

    protected abstract void downloadFileImpl(Path remotePath, Path localPath) throws BackupRestoreException;

    @Override
    public void uploadFile(Path localPath, Path remotePath, AbstractBackupPath path) throws BackupRestoreException{
        if (!localPath.toFile().exists())
            throw new BackupRestoreException("File do not exist: {}" + localPath);

        logger.info("Uploading file: {} to location: {}", localPath, remotePath);
        try{
            notifyEventStart(new BackupEvent(path));
            //TODO: Retries should ideally go here.
            long uploadedFileSize = uploadFileImpl(localPath, remotePath);
            backupMetrics.recordUploadRate(uploadedFileSize);
            notifyEventSuccess(new BackupEvent(path));
            logger.info("Successfully uploaded file: {} to location: {}", localPath, remotePath);
        }catch (BackupRestoreException e){
            backupMetrics.incrementInvalidUploads();
            notifyEventFailure(new BackupEvent(path));
            logger.error("Error while uploading file: {} to location: {}", localPath, remotePath);
            throw e;
        }
    }

    protected abstract long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException;

    @Override
    public final void addObserver(EventObserver<BackupEvent> observer) {
        if (observer == null)
            throw new NullPointerException("observer must not be null.");

        observers.addIfAbsent(observer);
    }

    @Override
    public void removeObserver(EventObserver<BackupEvent> observer) {
        if (observer == null)
            throw new NullPointerException("observer must not be null.");

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
}
