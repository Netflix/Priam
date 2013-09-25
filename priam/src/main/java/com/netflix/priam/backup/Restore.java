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

import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.ThreadSleeper;

/**
 * Main class for restoring data from backup
 */
@Singleton
public class Restore extends AbstractRestore
{
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);
    private final ICassandraProcess cassProcess;
    @Inject
    private Provider<AbstractBackupPath> pathProvider;
    @Inject
    private RestoreTokenSelector tokenSelector;
    @Inject
    private MetaData metaData;
    @Inject
    private InstanceIdentity id;

    @Inject
    public Restore(IConfiguration config, @Named("backup")IBackupFileSystem fs,Sleeper sleeper, ICassandraProcess cassProcess)
    {
        super(config, fs, JOBNAME, sleeper);
        this.cassProcess = cassProcess;
    }

    @Override
    public void execute() throws Exception
    {
        if (isRestoreEnabled(config))
        {
            logger.info("Starting restore for " + config.getRestoreSnapshot());
            String[] restore = config.getRestoreSnapshot().split(",");
            AbstractBackupPath path = pathProvider.get();
            final Date startTime = path.parseDate(restore[0]);
            final Date endTime = path.parseDate(restore[1]);
            String origToken = id.getInstance().getToken();
            try
            {
                if (config.isRestoreClosestToken())
                {
                    restoreToken = tokenSelector.getClosestToken(new BigInteger(origToken), startTime);
                    id.getInstance().setToken(restoreToken.toString());
                }
                new RetryableCallable<Void>()
                {
                    public Void retriableCall() throws Exception
                    {
                        logger.info("Attempting restore");
                        restore(startTime, endTime);
                        logger.info("Restore completed");
                        // Wait for other server init to complete
                        sleeper.sleep(30000);
                        return null;
                    }
                }.call();
            }
            finally
            {
                id.getInstance().setToken(origToken);
            }
        }
        cassProcess.start(true);
    }

    /**
     * Restore backup data for the specified time range
     */
    public void restore(Date startTime, Date endTime) throws Exception
    {
        // Stop cassandra if its running and restoring all keyspaces
        if (config.getRestoreKeySpaces().size() == 0)
            cassProcess.stop();

        // Cleanup local data
        SystemUtils.cleanupDir(config.getDataFileLocation(), config.getRestoreKeySpaces());

        // Try and read the Meta file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        String prefix = "";
        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();
        logger.info("Looking for meta file here:  " + prefix);
        Iterator<AbstractBackupPath> backupfiles = fs.list(prefix, startTime, endTime);
        while (backupfiles.hasNext())
        {
            AbstractBackupPath path = backupfiles.next();
            if (path.type == BackupFileType.META)
                metas.add(path);
        }
        
        if (metas.size() == 0)
        {
        	logger.info("[cass_backup] No meta file found, Restore Failed.");
        	assert false : "[cass_backup] No snapshots found, Restore Failed.";
        	return;
        }
        

        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Meta file for restore " + meta.getRemotePath());

        // Download snapshot which is listed in the meta file.
        List<AbstractBackupPath> snapshots = metaData.get(meta);
        download(snapshots.iterator(), BackupFileType.SNAP);

        logger.info("Downloading incrementals");
        // Download incrementals (SST).
        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.time, endTime);
        download(incrementals, BackupFileType.SST);
        
        //Downloading CommitLogs
        if (config.isBackingUpCommitLogs())  //TODO: will change to isRestoringCommitLogs()
        {	
        	logger.info("Delete all backuped commitlog files in " + config.getBackupCommitLogLocation());
        	SystemUtils.cleanupDir(config.getBackupCommitLogLocation(), null);
        	     
        	logger.info("Delete all commitlog files in " + config.getCommitLogLocation());
        	SystemUtils.cleanupDir(config.getCommitLogLocation(), null);
        	
        	Iterator<AbstractBackupPath> commitLogPathIterator = fs.list(prefix, meta.time, endTime); 
        	download(commitLogPathIterator, BackupFileType.CL, config.maxCommitLogsRestore());       	
        }
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public int getActiveCount()
    {
        return (executor == null) ? 0 : executor.getActiveCount();
    }

    public static boolean isRestoreEnabled(IConfiguration conf)
    {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac = (CollectionUtils.isEmpty(conf.getBackupRacs()) || conf.getBackupRacs().contains(conf.getRac()));
        return (isRestoreMode && isBackedupRac);
    }

}
