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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.priam.notification.BackupNotificationMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;

/*
 * Incremental/SSTable backup
 */
@Singleton
public class IncrementalBackup extends AbstractBackup implements IIncrementalBackup
{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    
    private final List<String> incrementalRemotePaths = new ArrayList<String>();
	private IncrementalMetaData metaData;

    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();

    @Inject
    public IncrementalBackup(IConfiguration config, Provider<AbstractBackupPath> pathFactory, @Named("backup") IFileSystemContext backupFileSystemCtx
    		, IncrementalMetaData metaData
			,BackupNotificationMgr backupNotificationMgr
			)
    {
        super(config, backupFileSystemCtx, pathFactory, backupNotificationMgr);
        this.metaData = metaData; //a means to upload audit trail (via meta_cf_yyyymmddhhmm.json) of files successfully uploaded)
		JOBNAME = "IncrementalBackup";
        init();
    }
    
    private void init() {
    	populateFilters();
    }

	@Override
	protected final String getConfigKeyspaceFilter(){
		return config.getIncrementalKeyspaceFilters();
	}

	@Override
	protected final String getConfigColumnfamilyFilter()
	{
		return config.getIncrementalCFFilter();
	}

    
    @Override
    public void execute() throws Exception
    {   	
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
            
				List<AbstractBackupPath> uploadedFiles = upload(backupDir, BackupFileType.SST);
            
				if ( ! uploadedFiles.isEmpty() ) {
					String incrementalUploadTime = AbstractBackupPath.formatDate(uploadedFiles.get(0).getTime()); //format of yyyymmddhhmm (e.g. 201505060901)
					String metaFileName = "meta_" + columnFamilyDir.getName() + "_" + incrementalUploadTime;
					logger.info("Uploading meta file for incremental backup: " + metaFileName); 
					this.metaData.setMetaFileName(metaFileName);
					this.metaData.set(uploadedFiles, incrementalUploadTime);
					logger.info("Uploaded meta file for incremental backup: " + metaFileName);                	
				}

			}
		}
 		
		if(incrementalRemotePaths.size() > 0)
		{
			notifyObservers();
		}

    }


    /**
     * Run every 10 Sec
     */
    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }
   
    public static void addObserver(IMessageObserver observer)
    {
    		observers.add(observer);
    }
    
    public static void removeObserver(IMessageObserver observer)
    {
    		observers.remove(observer);
    }
    
    public void notifyObservers()
    {
        for(IMessageObserver observer : observers)
        {
        		if(observer != null)
        		{
        			logger.debug("Updating incremental observers now ...");
        			observer.update(BACKUP_MESSAGE_TYPE.INCREMENTAL,incrementalRemotePaths);
        		}
        		else
        			logger.info("Observer is Null, hence can not notify ...");
        }
    }

	@Override
	protected void addToRemotePath(String remotePath) {
		incrementalRemotePaths.add(remotePath);		
	}

	@Override
	public long getNumPendingFiles() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getJobName() {
		return JOBNAME;
	}

}