/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.backup.parallel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.priam.notification.BackupNotificationMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackup;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.backup.IIncrementalBackup;
import com.netflix.priam.backup.IncrementalMetaData;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;

@Singleton
public class IncrementalBackupProducer extends AbstractBackup implements IIncrementalBackup {

	public static final String JOBNAME = "INCR_PARALLEL_BACKUP_THREAD";
	private static final Logger logger = LoggerFactory.getLogger(IncrementalBackupProducer.class);
	 
    private final List<String> incrementalRemotePaths = new ArrayList<String>();
    private final Map<String, Object> incrementalCFFilter = new HashMap<String, Object>();
	private final Map<String, Object> incrementalKeyspaceFilter  = new HashMap<String, Object>();

	private IncrementalMetaData metaData;
	private IncrementalConsumerMgr incrementalConsumerMgr;
	private ITaskQueueMgr<AbstractBackupPath> taskQueueMgr;
    
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
    	populateIncrementalFilters(); 
    	//"this" is a producer, lets wake up the "consumers"
    	this.incrementalConsumerMgr = new IncrementalConsumerMgr(this.taskQueueMgr, backupFileSystemCtx.getFileStrategy(config), super.config, super.backupNotificationMgr);
    	Thread consumerMgr = new Thread(this.incrementalConsumerMgr);
    	consumerMgr.start();
    	
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
        File dataDir = new File(config.getDataFileLocation());
        if (!dataDir.exists())
        {
            throw new IllegalArgumentException("The configured 'data file location' does not exist: "
                    + config.getDataFileLocation());
        }
        logger.debug("Scanning for backup in: {}", dataDir.getAbsolutePath());
        for (File keyspaceDir : dataDir.listFiles())
        {
        	if (keyspaceDir.isFile())
    			continue;
        	
        	if ( isFiltered(DIRECTORYTYPE.KEYSPACE, keyspaceDir.getName()) ) { //keyspace filtered?
            	logger.info(keyspaceDir.getName() + " is part of keyspace filter, incremental not done.");
            	continue;
            }
        	
        	for (File columnFamilyDir : keyspaceDir.listFiles())
            {
                if ( isFiltered(DIRECTORYTYPE.CF, keyspaceDir.getName(), columnFamilyDir.getName()) ) { //CF filtered?
                	logger.info("keyspace: " + keyspaceDir.getName() 
                			+ ", CF: " + columnFamilyDir.getName() + " is part of CF filter list, incrmental not done.");
                	continue;
                }
                
                File backupDir = new File(columnFamilyDir, "backups");
                if (!isValidBackupDir(keyspaceDir, columnFamilyDir, backupDir)) {
                	continue;
                }
                
                for (final File file : backupDir.listFiles()){
                    try
                    {
                        final AbstractBackupPath bp = pathFactory.get();
                        bp.parseLocal(file, BackupFileType.SST);
                        
                		this.taskQueueMgr.add(bp); //producer -- populate the queue of files.  *Note: producer will block if queue is full.
                    	
                    }
                    catch(Exception e)
                    {
                		logger.warn("Unable to queue incremental file, treating as non-fatal and moving on to next.  Msg: "
                				+ e.getLocalizedMessage()
                				+ " Fail to queue file: "
                				+ file.getAbsolutePath());
                    }

                } //end enqueuing all incremental files for a CF
            } //end processing all CFs for keyspace
        } //end processing keyspaces under the C* data dir
     
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
		
    /*	
     * 
     * @return true if directory should be filter from processing; otherwise, false.
     */
    private boolean isFiltered(DIRECTORYTYPE directoryType, String...args) {
    	if (directoryType.equals(DIRECTORYTYPE.CF)) {
    		String keyspaceName = args[0];
    		String cfName = args[1];
    		if (this.incrementalKeyspaceFilter.containsKey(keyspaceName)) { //account for keyspace which we want to filter
    			return true;
    		} else {
    			StringBuffer strBuf = new StringBuffer();
    			strBuf.append(keyspaceName);
    			strBuf.append('.');
    			strBuf.append(cfName);
            	return this.incrementalCFFilter.containsKey(strBuf.toString());    			
    		}
        	
    	} else if (directoryType.equals(DIRECTORYTYPE.KEYSPACE)) {
    		return this.incrementalKeyspaceFilter.containsKey(args[0]);
    		
    	} else {
    		throw new UnsupportedOperationException("Directory type not supported.  Invalid input: " + directoryType.name());
    	}

    }

	@Override
	/*
	 * @return an identifier of purpose of the task.
	 */
	public String getName() {
		return JOBNAME;
	}
	
    private void populateIncrementalFilters() {
    	String keyspaceFilters = this.config.getIncrementalKeyspaceFilters();
    	if (keyspaceFilters == null || keyspaceFilters.isEmpty()) {
    		
    		logger.info("No keyspace filter set for incremental.");
    		
    	} else {

        	String[] keyspaces = keyspaceFilters.split(",");
        	for (int i=0; i < keyspaces.length; i++ ) {
        		logger.info("Adding incremental keyspace filter: " + keyspaces[i]);
        		this.incrementalKeyspaceFilter.put(keyspaces[i], null);
        	}    		
    		
    	}
    	
    	String cfFilters = this.config.getIncrementalCFFilter();
    	if (cfFilters == null || cfFilters.isEmpty()) {
    		
    		logger.info("No column family filter set for snapshot.");
    		
    	} else {

        	String[] cf = cfFilters.split(",");
        	for (int i=0; i < cf.length; i++) {
        		if (isValidCFFilterFormat(cf[i])) {
        			logger.info("Adding incremental CF filter: " + cf[i]);
            		this.incrementalCFFilter.put(cf[i], null);        			
        		} else {
        			throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + cf[i]);
        		}
        	}    		
    		
    	}    	
    }

	@Override
	public long getNumPendingFiles() {
		throw new UnsupportedOperationException();
	}
	
    /**
     * Run every 10 Sec
     */
    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, INCREMENTAL_INTERVAL_IN_MILLISECS);
    }
    
	@Override
	public String getJobName() {
		return JOBNAME;
	}

}