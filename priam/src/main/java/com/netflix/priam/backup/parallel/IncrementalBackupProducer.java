/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup.parallel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.priam.backup.*;
import com.netflix.priam.notification.BackupNotificationMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;

@Singleton
public class IncrementalBackupProducer extends AbstractBackup implements IIncrementalBackup {

    public static final String JOBNAME = "ParallelIncremental";
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackupProducer.class);

    private final List<String> incrementalRemotePaths = new ArrayList<String>();
    private IncrementalMetaData metaData;
    private IncrementalConsumerMgr incrementalConsumerMgr;
    private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;
    private BackupRestoreUtil backupRestoreUtil;

    @Inject
    public IncrementalBackupProducer(IConfiguration config, Provider<AbstractBackupPath> pathFactory, @Named("backup") IFileSystemContext backupFileSystemCtx
            , IncrementalMetaData metaData
            , @Named("backup") ITaskQueueMgr taskQueueMgr
            , BackupNotificationMgr backupNotificationMgr
    ) {

        super(config, backupFileSystemCtx, pathFactory, backupNotificationMgr);
        this.taskQueueMgr = taskQueueMgr;
        this.metaData = metaData;

        init(backupFileSystemCtx);
    }

    private void init(IFileSystemContext backupFileSystemCtx) {
        backupRestoreUtil = new BackupRestoreUtil(config.getIncrementalKeyspaceFilters(), config.getIncrementalCFFilter());
        //"this" is a producer, lets wake up the "consumers"
        this.incrementalConsumerMgr = new IncrementalConsumerMgr(this.taskQueueMgr, backupFileSystemCtx.getFileStrategy(config), super.config);
        Thread consumerMgr = new Thread(this.incrementalConsumerMgr);
        consumerMgr.start();

    }

    @Override
    protected void backupUploadFlow(File backupDir) throws Exception {
        for (final File file : backupDir.listFiles()) {
            try {
                final AbstractBackupPath bp = pathFactory.get();
                bp.parseLocal(file, BackupFileType.SST);
                this.taskQueueMgr.add(bp); //producer -- populate the queue of files.  *Note: producer will block if queue is full.
            } catch (Exception e) {
                logger.warn("Unable to queue incremental file, treating as non-fatal and moving on to next.  Msg: "
                        + e.getLocalizedMessage()
                        + " Fail to queue file: "
                        + file.getAbsolutePath());
            }

        } //end enqueuing all incremental files for a CF
    }

    @Override
    /*
	 * Keeping track of successfully uploaded files.
	 */
    protected void addToRemotePath(String remotePath) {
        incrementalRemotePaths.add(remotePath);
    }


    @Override
    public void execute() throws Exception {
        //Clearing remotePath List
        incrementalRemotePaths.clear();
        initiateBackup("backups", backupRestoreUtil);
        return;
    }

    public void postProcessing() {
        /*
        *
        * Upload the audit file of completed uploads
        *
		List<AbstractBackupPath> uploadedFiles = upload(backupDir, BackupFileType.SST);                 
		if ( ! uploadedFiles.isEmpty() ) {
   		String incrementalUploadTime = AbstractBackupPath.formatDate(uploadedFiles.get(0).getTime()); //format of yyyymmddhhmm (e.g. 201505060901)
   		String metaFileName = "meta_" + columnFamilyDir.getName() + "_" + incrementalUploadTime;
   		logger.info("Uploading meta file for incremental backup: " + metaFileName); 
   		this.metaData.setMetaFileName(metaFileName);
   		this.metaData.set(uploadedFiles, incrementalUploadTime);
   		logger.info("Uploaded meta file for incremental backup: " + metaFileName);                	
		}
        */

    	/* *
    	 * Notify observers once all incrremental uploads completed
    	 * 
        if(incrementalRemotePaths.size() > 0)
        {
        	notifyObservers();
        }
        * */

    }

    @Override
	/*
	 * @return an identifier of purpose of the task.
	 */
    public String getName() {
        return JOBNAME;
    }

    @Override
    public long getNumPendingFiles() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return Timer that run every 10 Sec
     */
    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME, INCREMENTAL_INTERVAL_IN_MILLISECS);
    }

    @Override
    public String getJobName() {
        return JOBNAME;
    }

}