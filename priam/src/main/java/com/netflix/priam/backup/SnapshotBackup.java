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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.SnapshotBackupMeasurement;
import com.netflix.priam.notification.BackupNotificationMgr;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackup.DIRECTORYTYPE;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.BackupStatusMgr.BackupMetadata;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.ThreadSleeper;

/**
 * Task for running daily snapshots
 */
@Singleton
public class SnapshotBackup extends AbstractBackup
{
    public static String JOBNAME = "SnapshotBackup";
    
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    private final MetaData metaData;
    private final List<String> snapshotRemotePaths = new ArrayList<String>();
    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();
    private final ThreadSleeper sleeper = new ThreadSleeper();
    private final long WAIT_TIME_MS = 60 * 1000 * 10;
    private final CommitLogBackup clBackup;
    private final Map<String, List<String>>  snapshotCFFilter = new HashMap<String, List<String>>(); //key: keyspace, value: a list of CFs within the keyspace
	private final Map<String, Object> snapshotKeyspaceFilter  = new HashMap<String, Object>(); //key: keyspace, value: null

	private BackupStatusMgr completedBackups;


    @Inject
    public SnapshotBackup(IConfiguration config, Provider<AbstractBackupPath> pathFactory, 
    		              MetaData metaData, CommitLogBackup clBackup, @Named("backup") IFileSystemContext backupFileSystemCtx
    		              ,BackupStatusMgr completedBackups
                          ,BackupNotificationMgr backupNotificationMgr
                        )
    {
        super(config, backupFileSystemCtx, pathFactory, backupNotificationMgr);
        this.metaData = metaData;
        this.clBackup = clBackup;
        this.completedBackups = completedBackups;
        init();
    }

    private void init() {
    	populateSnapshotFilters();
    }
    
    private void populateSnapshotFilters() {
    	String keyspaceFilters = this.config.getSnapshotKeyspaceFilters();
    	if (keyspaceFilters == null || keyspaceFilters.isEmpty()) {
    		
    		logger.info("No keyspace filter set for snapshot.");
    		
    	} else {

        	String[] keyspaces = keyspaceFilters.split(",");
        	for (int i=0; i < keyspaces.length; i++ ) {
        		logger.info("Adding snapshot keyspace filter: " + keyspaces[i]);
        		this.snapshotKeyspaceFilter.put(keyspaces[i], null);
        	}    		
    		
    	}
    	
    	String cfFilters = this.config.getSnapshotCFFilter();
    	if (cfFilters == null || cfFilters.isEmpty()) {
    		
    		logger.info("No column family filter set for snapshot.");
    		
    	} else {

    		String[] filters = cfFilters.split(",");
    		for (int i=0; i < filters.length; i++) { //process each filter
    			
    			if (isValidCFFilterFormat(filters[i])) {
    				
        			String[] filter = filters[i].split("\\.");
        			String ksName = filter[0];
        			String cfName = filter[1];
        			logger.info("Adding snapshot CF filter, keyspaceName: " + ksName + ", cf: " + cfName);
        			
        			if (this.snapshotCFFilter.containsKey(ksName)) {
        				//add cf to existing filter
        				List<String> cfs = this.snapshotCFFilter.get(ksName);
        				cfs.add(cfName);
        				this.snapshotCFFilter.put(ksName, cfs);
        				
        			} else {
        				
        				List<String> cfs = new ArrayList<String>();
        				cfs.add(cfName);
        				this.snapshotCFFilter.put(ksName, cfs);
        				
        			}
    				
    			} else {
    				throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + filters[i]);
    			}
    			
    		} //end processing each filter
    	}    	
    }
    
    @Override
    public void execute() throws Exception
    {
        //If Cassandra is started then only start Snapshot Backup
    		while(!CassandraMonitor.isCassadraStarted())
    		{
        		logger.debug("Cassandra is not yet started, hence Snapshot Backup will start after ["+WAIT_TIME_MS/1000+"] secs ...");
    			sleeper.sleep(WAIT_TIME_MS);
    		}

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        Date startTime = cal.getTime();
        String snapshotName = pathFactory.get().formatDate(cal.getTime());
        try
        {
            logger.info("Starting snapshot " + snapshotName);
            //Clearing remotePath List
            snapshotRemotePaths.clear();
            takeSnapshot(snapshotName);
            // Collect all snapshot dir's under keyspace dir's
            List<AbstractBackupPath> bps = Lists.newArrayList();
            File dataDir = new File(config.getDataFileLocation());
            for (File keyspaceDir : dataDir.listFiles())
            {
                if (keyspaceDir.isFile())
                		continue;
                
                if ( isFiltered(DIRECTORYTYPE.KEYSPACE, keyspaceDir.getName())  ) { //keyspace filtered?
                	logger.info(keyspaceDir.getName() + " is part of keyspace filter, will not be backed up.");
                	continue;
                }
                
                
                logger.debug("Entering {} keyspace..", keyspaceDir.getName());
                for (File columnFamilyDir : keyspaceDir.listFiles())
                {
                	if ( isFiltered(DIRECTORYTYPE.CF, keyspaceDir.getName(), columnFamilyDir.getName() )) { //CF filtered?
                    	logger.info("keyspace: " + keyspaceDir.getName() 
                    			+ ", CF: " + columnFamilyDir.getName() + " is part of CF filter list, will not be backed up.");
                    	continue;
                	}
                		
                    logger.debug("Entering {} columnFamily..", columnFamilyDir.getName());
                    File snpDir = new File(columnFamilyDir, "snapshots");
                    if (!isValidBackupDir(keyspaceDir, columnFamilyDir, snpDir)) {
                    	continue;
                    }
                        
                    File snapshotDir = getValidSnapshot(columnFamilyDir, snpDir, snapshotName);
                    // Add files to this dir
                    if (null != snapshotDir)
                    	bps.addAll(upload(snapshotDir, BackupFileType.SNAP));
                    else
                    	logger.warn("{} folder does not contain {} snapshots", snpDir, snapshotName);
                }
                
            }
            // Upload meta file
            AbstractBackupPath metaJson = metaData.set(bps, snapshotName);
            logger.info("Snapshot upload complete for " + snapshotName);
            Calendar completed = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            this.postProcesing(snapshotName, startTime, completed.getTime(), metaJson);
            
            if(snapshotRemotePaths.size() > 0)
            {
            	notifyObservers();
            }
         
        }
        finally
        {
            try
            {
                clearSnapshot(snapshotName);  
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
            }
        }
    }
    
    /*
     * Performs any post processing (e.g. log success of backup).
     * 
     * @param name of the snapshotname, format is yyyymmddhhs
     * @param start time of backup
     */
    private void postProcesing(String snapshotname, Date start, Date completed, AbstractBackupPath adp) throws JSONException {
        String key = BackupStatusMgr.formatKey(IMessageObserver.BACKUP_MESSAGE_TYPE.SNAPSHOT, start);  //format is backuptype_yyyymmdd
        BackupMetadata metadata = this.completedBackups.add(key, snapshotname, start, completed);

        backupNotificationMgr.notify(adp, BackupNotificationMgr.SUCCESS_VAL);
    }
    
    /*
     * @param keyspace or columnfamily directory type.
     * @return true if directory should be filter from processing; otherwise, false.
     */
    private boolean isFiltered(DIRECTORYTYPE directoryType, String...args) {
    	
    	if (directoryType.equals(DIRECTORYTYPE.KEYSPACE)) { //start with filtering the parent (keyspace)
    		//Apply each keyspace filter to input string
    		String keyspaceName = args[0];
    		
    		java.util.Set<String> ksFilters = this.snapshotKeyspaceFilter.keySet();
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
    		if ( !this.snapshotCFFilter.containsKey(keyspaceName) ) {
    			return false;
    		}
    		
    		String cfName = args[1];
    		List<String> cfsFilter = this.snapshotCFFilter.get(keyspaceName);
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

    private File getValidSnapshot(File columnFamilyDir, File snpDir, String snapshotName)
    {
        for (File snapshotDir : snpDir.listFiles())
            if (snapshotDir.getName().matches(snapshotName))
                return snapshotDir;
        return null;
    }

    private void takeSnapshot(final String snapshotName) throws Exception
    {
        new RetryableCallable<Void>()
        {
            public Void retriableCall() throws Exception
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.takeSnapshot(snapshotName, null, new String[0]);
                return null;
            }
        }.call();
    }

    private void clearSnapshot(final String snapshotTag) throws Exception
    {
        new RetryableCallable<Void>()
        {
            public Void retriableCall() throws Exception
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.clearSnapshot(snapshotTag);
                return null;
            }
        }.call();
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config)
    {
        int hour = config.getBackupHour();
        return new CronTimer(JOBNAME, hour, 1, 0);
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
        			logger.debug("Updating snapshot observers now ...");
        			observer.update(BACKUP_MESSAGE_TYPE.SNAPSHOT,snapshotRemotePaths);
        		}
        		else
        			logger.info("Observer is Null, hence can not notify ...");
        }
    }

	@Override
	protected void addToRemotePath(String remotePath) {		
		snapshotRemotePaths.add(remotePath);		
	}

}
