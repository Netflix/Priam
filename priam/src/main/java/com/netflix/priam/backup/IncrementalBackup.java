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
    public static final String JOBNAME = "INCR_BACKUP_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    
    private final List<String> incrementalRemotePaths = new ArrayList<String>();
	private IncrementalMetaData metaData;
	
	private final Map<String, List<String>> incrementalCFFilter = new HashMap<String, List<String>>(); //key: keyspace, value: a list of CFs within the keyspace
	private final Map<String, Object> incrementalKeyspaceFilter  = new HashMap<String, Object>(); //key: keyspace, value: null

    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();

    @Inject
    public IncrementalBackup(IConfiguration config, Provider<AbstractBackupPath> pathFactory, @Named("backup") IFileSystemContext backupFileSystemCtx
    		, IncrementalMetaData metaData
			,BackupNotificationMgr backupNotificationMgr
			)
    {
        super(config, backupFileSystemCtx, pathFactory, backupNotificationMgr);
        this.metaData = metaData; //a means to upload audit trail (via meta_cf_yyyymmddhhmm.json) of files successfully uploaded)
        
        init();
    }
    
    private void init() {
    	populateIncrementalFilters();
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
    		
    		logger.info("No column family filter set for incremental.");
    		
    	} else {

    		String[] filters = cfFilters.split(",");
    		for (int i=0; i < filters.length; i++) { //process each filter
    			if (isValidCFFilterFormat(filters[i])) {
    				
        			String[] filter = filters[i].split("\\.");
        			String ksName = filter[0];
        			String cfName = filter[1];
        			logger.info("Adding incremental CF filter, keyspaceName: " + ksName + ", cf: " + cfName);
        			
        			if (this.incrementalCFFilter.containsKey(ksName)) {
        				//add cf to existing filter
        				List<String> cfs = this.incrementalCFFilter.get(ksName);
        				cfs.add(cfName);
        				this.incrementalCFFilter.put(ksName, cfs);
        				
        			} else {
        				
        				List<String> cfs = new ArrayList<String>();
        				cfs.add(cfName);
        				this.incrementalCFFilter.put(ksName, cfs);
        				
        			}
        			
    			}  else {
    				throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + filters[i]);
    			}
    		} //end processing each filter		
    		
    	}    	
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
    
    /*
     * @return true if directory should be filter from processing; otherwise, false.
     */
    private boolean isFiltered(DIRECTORYTYPE directoryType, String...args) {
    	if (directoryType.equals(DIRECTORYTYPE.KEYSPACE)) { //start with filtering the parent (keyspace)
    		String keyspaceName = args[0];
    		//Apply each keyspace filter to input string
    		java.util.Set<String> ksFilters = this.incrementalKeyspaceFilter.keySet();
    		Iterator<String> it = ksFilters.iterator();
    		while (it.hasNext()) {
    			String ksFilter = it.next();
    			Pattern p = Pattern.compile(ksFilter);
    			Matcher m = p.matcher(keyspaceName);
    			if (m.find()) {
    				logger.info("Keyspace: " + keyspaceName + " matched filter: " + ksFilter);
    				return true;
    			}
    		}    
    		
    	}
    	
    	if (directoryType.equals(DIRECTORYTYPE.CF)) { //parent (keyspace) is not filtered, now see if the child (CF) is filtered
    		String keyspaceName = args[0];
    		if ( !this.incrementalCFFilter.containsKey(keyspaceName) ) {
    			return false;
    		}
    		
    		String cfName = args[1];
    		List<String> cfsFilter = this.incrementalCFFilter.get(keyspaceName);
			for (int i=0; i < cfsFilter.size(); i++) {
				Pattern p = Pattern.compile(cfsFilter.get(i));
				Matcher m = p.matcher(cfName);
				if (m.find()) {
    				logger.info(keyspaceName + "." +  cfName + " matched filter");
    				return true;
				}
			}
    	} 
    	
    	return false; //if here, current input are not part of keyspae and cf filters

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