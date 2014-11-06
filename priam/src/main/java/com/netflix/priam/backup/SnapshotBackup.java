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
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
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
    

    @Inject
    public SnapshotBackup(IConfiguration config, Provider<AbstractBackupPath> pathFactory, 
    		              MetaData metaData, CommitLogBackup clBackup, @Named("backup") IFileSystemContext backupFileSystemCtx)
    {
    	super(config, backupFileSystemCtx, pathFactory);
        this.metaData = metaData;
        this.clBackup = clBackup;
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
                logger.debug("Entering {} keyspace..", keyspaceDir.getName());
                for (File columnFamilyDir : keyspaceDir.listFiles())
                {
                    logger.debug("Entering {} columnFamily..", columnFamilyDir.getName());
                    File snpDir = new File(columnFamilyDir, "snapshots");
                    if (!isValidBackupDir(keyspaceDir, columnFamilyDir, snpDir))
                        continue;
                    File snapshotDir = getValidSnapshot(columnFamilyDir, snpDir, snapshotName);
                    // Add files to this dir
                    if (null != snapshotDir)
                        bps.addAll(upload(snapshotDir, BackupFileType.SNAP));
                    else
                        logger.warn("{} folder does not contain {} snapshots", snpDir, snapshotName);
                }
            }
            // Upload meta file
            metaData.set(bps, snapshotName);
            logger.info("Snapshot upload complete for " + snapshotName);
            
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
        return new CronTimer(hour, 1, 0);
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
