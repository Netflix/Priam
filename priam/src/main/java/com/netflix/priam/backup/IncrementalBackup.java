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
import java.util.List;

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
public class IncrementalBackup extends AbstractBackup
{
    public static final String JOBNAME = "INCR_BACKUP_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    private final List<String> incrementalRemotePaths = new ArrayList<String>();
    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();

    @Inject
    public IncrementalBackup(IConfiguration config, @Named("backup")IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory)
    {
        super(config, fs, pathFactory);
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
            for (File columnFamilyDir : keyspaceDir.listFiles())
            {
                File backupDir = new File(columnFamilyDir, "backups");
                if (!isValidBackupDir(keyspaceDir, columnFamilyDir, backupDir))
                    continue;
                upload(backupDir, BackupFileType.SST);
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

}
