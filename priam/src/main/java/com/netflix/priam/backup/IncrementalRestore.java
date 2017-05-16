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
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.cassandra.io.sstable.SSTableLoaderWrapper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.IMessageObserver.RESTORE_MESSAGE_STATUS;
import com.netflix.priam.backup.IMessageObserver.RESTORE_MESSAGE_TYPE;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.Sleeper;


/*
 * Incremental SSTable Restore using SSTable Loader
 */
@Singleton
public class IncrementalRestore extends AbstractRestore
{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalRestore.class);
    public static final String JOBNAME = "INCR_RESTORE_THREAD";
    private static final String SYSTEM_KEYSPACE = "system";
    private final List<String> streamedIncrementalRestorePaths = new ArrayList<String>();
    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();

    /* Marked public for testing */
    public static final Pattern SECONDRY_INDEX_PATTERN = Pattern.compile("^[a-zA-Z_0-9-]+\\.[a-zA-Z_0-9-]+\\.[a-z1-9]{2,4}$");

    private final File restoreDir;
    
    @Inject
    private SSTableLoaderWrapper loader;
    
    @Inject
    private PriamServer priamServer;
    
    @Inject
    public IncrementalRestore(IConfiguration config, @Named("incr_restore")IBackupFileSystem fs, Sleeper sleeper)
    {
        super(config, fs,JOBNAME, sleeper);
        this.restoreDir = new File(config.getDataFileLocation(), "restore_incremental");
    }

    @Override
    public void execute() throws Exception
    {
        String prefix = config.getRestorePrefix();
        if (Strings.isNullOrEmpty(prefix))
        {
            logger.error("Restore prefix is not set, skipping incremental restore to avoid looping over the incremental backups. Plz check the configurations");
            return; // No point in restoring the files which was just backedup.
        }

        if (tracker.isEmpty()) {
            logger.error("You should perform full restore before incremental restore, skipping incremental restore.");
            return;
        }

        if (config.isRestoreClosestToken())
        {
            priamServer.getId().getInstance().setToken(restoreToken.toString());
        }

        Date start = tracker.first().time;
        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, start, Calendar.getInstance().getTime());
        FileUtils.createDirectory(restoreDir); // create restore dir.
        while (incrementals.hasNext())
        {
            AbstractBackupPath temp = incrementals.next();
            if (tracker.contains(temp) || start.compareTo(temp.time) >= 0)
                continue; // ignore the ones which where already downloaded.
            if (temp.getType() != BackupFileType.SST)
                continue; // download SST's only.
            // skip System Keyspace, else you will run into concurrent schema issues.
            if (temp.getKeyspace().equalsIgnoreCase("System"))
                continue;
            /* Cassandra will rebuild Secondary index's after streaming is complete so we can ignore those */
            if (SECONDRY_INDEX_PATTERN.matcher(temp.fileName).matches()) // Make this use the constant from 1.1
                continue;
            
            // Create Directory for Individual Token respective to each incoming file
            File tokenDir = new File(restoreDir, temp.getToken());
            FileUtils.createDirectory(tokenDir);
            File keyspaceDir = config.getTargetKSName() == null ? new File(tokenDir, temp.keyspace) : new File(tokenDir, config.getTargetKSName());
            FileUtils.createDirectory(keyspaceDir);
            File columnFamilyDir = config.getTargetCFName() == null ? new File(keyspaceDir, temp.columnFamily) : new File(tokenDir, config.getTargetCFName());
            FileUtils.createDirectory(columnFamilyDir);
            logger.debug("*** Keyspace = "+keyspaceDir.getAbsolutePath()+ " Column Family = "+columnFamilyDir.getAbsolutePath()+" File = "+temp.getRemotePath());
            if(config.getTargetKSName() != null || config.getTargetCFName() != null)
            	 	temp.fileName = renameIncrementalRestoreFile(temp.fileName);
            download(temp, new File(columnFamilyDir, temp.fileName));
        }
        // wait for all the downloads in this batch to complete.
        waitToComplete();
        // stream the SST's in the dir
        for (File tokenDir : restoreDir.listFiles())
        {
            for (File keyspaceDir : tokenDir.listFiles())
            {
            		for(File columnFamilyDir : keyspaceDir.listFiles())
            		{
            			Collection<PendingFile> streamedSSTs = loader.stream(columnFamilyDir);
            			addToStreamedIncrementalRestorePaths(streamedSSTs);
            			if(streamedIncrementalRestorePaths.size() > 0)
            			{
            				logger.debug("streamedIncrementalRestorePaths > 0, hence notifying observers");
            				notifyStreamedDataObservers();
            			}
            			// cleanup the dir which where streamed.
            			loader.deleteCompleted(streamedSSTs);
            		}
            }
        }
    }

    /**
     * Run every 20 Sec
     */
    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 20L * 1000);
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
    
    public void addToStreamedIncrementalRestorePaths(Collection<PendingFile> streamedSSTs)
    {
    		streamedIncrementalRestorePaths.clear();
    		for(PendingFile streamedSST:streamedSSTs)
    			streamedIncrementalRestorePaths.add(streamedSST.getFilename());
    }
    
    public void notifyStreamedDataObservers()
    {
        for(IMessageObserver observer : observers)
        {
        		if(observer != null)
        		{
        			observer.update(RESTORE_MESSAGE_TYPE.INCREMENTAL,streamedIncrementalRestorePaths,RESTORE_MESSAGE_STATUS.STREAMED);
        		}
        		else
        			logger.error("Streamed Observer is Null, hence can not notify ...");
        }
    }
    
    public String renameIncrementalRestoreFile(String fileToBeRenamed)
    {
		String[] splitToBeRenamed = StringUtils.split(fileToBeRenamed, '-');
		
		// Rename KS
		if (config.getTargetKSName() != null)
		{
			logger.info("Old Keyspace = ["+splitToBeRenamed[0]+"] --> New Keyspace = ["+config.getTargetKSName()+"]");
			splitToBeRenamed[0] = config.getTargetKSName();
		}
		
		// Rename CF
		if(config.getTargetCFName() != null)
		{
			logger.info("Old Column Family = ["+splitToBeRenamed[1]+"] --> New Column Family = ["+config.getTargetCFName()+"]");
			splitToBeRenamed[1] = config.getTargetCFName();
		}
		
		logger.info("Orig Filename = ["+fileToBeRenamed+"] --> New Filename = ["+StringUtils.join(splitToBeRenamed,'-')+"]");

		return StringUtils.join(splitToBeRenamed,'-');

    }

}
