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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Incremental/SSTable backup
 */
@Singleton
public class IncrementalBackup extends AbstractBackup {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    public static final String JOBNAME = "IncrementalBackup";
    private final List<String> incrementalRemotePaths = new ArrayList<>();
    private final IncrementalMetaData metaData;
    private final BackupRestoreUtil backupRestoreUtil;
    private static final List<IMessageObserver> observers = new ArrayList<>();

    @Inject
    public IncrementalBackup(
            IConfiguration config,
            Provider<AbstractBackupPath> pathFactory,
            IFileSystemContext backupFileSystemCtx,
            IncrementalMetaData metaData) {
        super(config, backupFileSystemCtx, pathFactory);
        // a means to upload audit trail (via meta_cf_yyyymmddhhmm.json) of files successfully
        // uploaded)
        this.metaData = metaData;
        backupRestoreUtil =
                new BackupRestoreUtil(
                        config.getIncrementalIncludeCFList(), config.getIncrementalExcludeCFList());
    }

    @Override
    public void execute() throws Exception {
        // Clearing remotePath List
        incrementalRemotePaths.clear();
        initiateBackup(INCREMENTAL_BACKUP_FOLDER, backupRestoreUtil);
        if (incrementalRemotePaths.size() > 0) {
            notifyObservers();
        }
    }

    /** Run every 10 Sec */
    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static void addObserver(IMessageObserver observer) {
        observers.add(observer);
    }

    public static void removeObserver(IMessageObserver observer) {
        observers.remove(observer);
    }

    private void notifyObservers() {
        for (IMessageObserver observer : observers) {
            if (observer != null) {
                logger.debug("Updating incremental observers now ...");
                observer.update(BACKUP_MESSAGE_TYPE.INCREMENTAL, incrementalRemotePaths);
            } else logger.info("Observer is Null, hence can not notify ...");
        }
    }

    @Override
    protected void processColumnFamily(String keyspace, String columnFamily, File backupDir)
            throws Exception {
        List<AbstractBackupPath> uploadedFiles =
                upload(backupDir, BackupFileType.SST, config.enableAsyncIncremental(), true);

        if (!uploadedFiles.isEmpty()) {
            // format of yyyymmddhhmm (e.g. 201505060901)
            String incrementalUploadTime =
                    AbstractBackupPath.formatDate(uploadedFiles.get(0).getTime());
            String metaFileName = "meta_" + columnFamily + "_" + incrementalUploadTime;
            logger.info("Uploading meta file for incremental backup: {}", metaFileName);
            this.metaData.setMetaFileName(metaFileName);
            this.metaData.set(uploadedFiles, incrementalUploadTime);
            logger.info("Uploaded meta file for incremental backup: {}", metaFileName);
        }
    }

    @Override
    protected void addToRemotePath(String remotePath) {
        incrementalRemotePaths.add(remotePath);
    }
}
