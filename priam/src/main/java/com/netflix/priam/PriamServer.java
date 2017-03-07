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
package com.netflix.priam;

import com.netflix.priam.cluster.management.FlushTask;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.aws.UpdateCleanupPolicy;
import com.netflix.priam.aws.UpdateSecuritySettings;
import com.netflix.priam.backup.CommitLogBackupTask;
import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.SnapshotBackup;
import com.netflix.priam.backup.parallel.IncrementalBackupProducer;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.restore.AwsCrossAccountCryptographyRestoreStrategy;
import com.netflix.priam.restore.EncryptedRestoreStrategy;
import com.netflix.priam.restore.GoogleCryptographyRestoreStrategy;
import com.netflix.priam.restore.RestoreContext;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TuneCassandra;

/**
 * Start all tasks here - Property update task - Backup task - Restore task -
 * Incremental backup
 */
@Singleton
public class PriamServer
{
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private final InstanceIdentity id;
    private final Sleeper sleeper;
    private final ICassandraProcess cassProcess;
    private static final int CASSANDRA_MONITORING_INITIAL_DELAY = 10;
    private static final Logger logger = LoggerFactory.getLogger(PriamServer.class);

    @Inject
    public PriamServer(IConfiguration config, PriamScheduler scheduler, InstanceIdentity id, Sleeper sleeper, ICassandraProcess cassProcess)
    {
        this.config = config;
        this.scheduler = scheduler;
        this.id = id;
        this.sleeper = sleeper;
        this.cassProcess = cassProcess;
    }

    public void intialize() throws Exception
    {     
        if (id.getInstance().isOutOfService())
            return;

        // start to schedule jobs
        scheduler.start();

        // update security settings.
        if (config.isMultiDC())
        {
            scheduler.runTaskNow(UpdateSecuritySettings.class);
            // sleep for 150 sec if this is a new node with new IP for SG to be updated by other seed nodes
            if (id.isReplace() || id.isTokenPregenerated())
            	sleeper.sleep(150 * 1000);
            else if (UpdateSecuritySettings.firstTimeUpdated)
                sleeper.sleep(60 * 1000);
            
            scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer(id));
        }

        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);

        // Determine if we need to restore from backup else start cassandra.
        if (!config.getRestoreSnapshot().equals("")) {

            if (config.getRestoreSourceType() == null || config.getRestoreSourceType().equals("") ) {
                //Restore is needed and it will be done from the primary AWS account
            	
                if (config.isEncryptBackupEnabled()) {
                	//Data needs to be decrypted as part of the restore.
	                scheduler.addTask(EncryptedRestoreStrategy.JOBNAME, EncryptedRestoreStrategy.class, EncryptedRestoreStrategy.getTimer());
	                logger.info("Scheduled task " + Restore.JOBNAME);

                } else {
                	//Data does NOT need to be decrypted as part of the restore.
	                scheduler.addTask(Restore.JOBNAME, Restore.class, Restore.getTimer());//restore from the AWS primary acct -- default
	                logger.info("Scheduled task " + Restore.JOBNAME);
	                
                }


            } else {
                //Restore is needed and it will be done either from Google or a non-primary AWS account. 

            	if ( config.isEncryptBackupEnabled() ) {
            		//Data needs to be decrypted as part of the restore.
            		
                    if (config.getRestoreSourceType().equalsIgnoreCase((RestoreContext.SourceType.AWSCROSSACCT.toString()) ) ) {
                        //Retore from a non-primary AWS account
                        scheduler.addTask(AwsCrossAccountCryptographyRestoreStrategy.JOBNAME, AwsCrossAccountCryptographyRestoreStrategy.class, AwsCrossAccountCryptographyRestoreStrategy.getTimer());
                        logger.info("Scheduled task " + AwsCrossAccountCryptographyRestoreStrategy.JOBNAME);

	                } else if (config.getRestoreSourceType().equalsIgnoreCase(RestoreContext.SourceType.GOOGLE.toString()) ) {
	                        //Restore from Google Cloud Storage (GCS)
	                        scheduler.addTask(GoogleCryptographyRestoreStrategy.JOBNAME, GoogleCryptographyRestoreStrategy.class, GoogleCryptographyRestoreStrategy.getTimer());
	                        logger.info("Scheduled task " + GoogleCryptographyRestoreStrategy.JOBNAME);
	
	                } else {
	                        throw new UnsupportedOperationException("Source type (" +  config.getRestoreSourceType() + ") for the scheduled restore not supported.");
	                }            		
            		
            	} else {
            		throw new UnsupportedOperationException("For this release, Source type (" +  config.getRestoreSourceType() + ") for the scheduled restore, we expect the data was encrypted.");
            	}

            }

        } else { //no restores needed
        	
            logger.info("No restore needed, task not scheduled");
             if(!config.doesCassandraStartManually())
            	 cassProcess.start(true);                                 // Start cassandra.
             else
                 logger.info("config.doesCassandraStartManually() is set to True, hence Cassandra needs to be started manually ...");
        }


        /*
         *  Run the delayed task (after 10 seconds) to Monitor Cassandra
         *  If Restore option is chosen, then Running Cassandra instance is stopped 
         *  Hence waiting for Cassandra to stop
         */
        scheduler.addTaskWithDelay(CassandraMonitor.JOBNAME,CassandraMonitor.class, CassandraMonitor.getTimer(), CASSANDRA_MONITORING_INITIAL_DELAY);
        
        // Start the snapshot backup schedule - Always run this. (If you want to
        // set it off, set backup hour to -1) or set backup cron to "0 0 5 31 2 ?" (Feb 31)
        if ((!StringUtils.isEmpty(config.getBackupCronExpression()) || config.getBackupHour() >= 0) && (CollectionUtils.isEmpty(config.getBackupRacs()) || config.getBackupRacs().contains(config.getRac())))
        {
            scheduler.addTask(SnapshotBackup.JOBNAME, SnapshotBackup.class, SnapshotBackup.getTimer(config));

            // Start the Incremental backup schedule if enabled
            if (config.isIncrBackup()) {
            	if ( !config.isIncrBackupParallelEnabled() ) {
            		scheduler.addTask(IncrementalBackup.JOBNAME, IncrementalBackup.class, IncrementalBackup.getTimer());
            		logger.info("Added incremental synchronous bkup");
            	} else {
            		scheduler.addTask(IncrementalBackupProducer.JOBNAME, IncrementalBackupProducer.class, IncrementalBackupProducer.getTimer());
            		logger.info("Added incremental async-synchronous bkup, next fired time: " + IncrementalBackupProducer.getTimer().getTrigger().getNextFireTime());
            	} 
            }

        }
       
        if (config.isBackingUpCommitLogs())
        {
        	scheduler.addTask(CommitLogBackupTask.JOBNAME, CommitLogBackupTask.class, CommitLogBackupTask.getTimer(config));
        }
        
        //Set cleanup
        scheduler.addTask(UpdateCleanupPolicy.JOBNAME, UpdateCleanupPolicy.class, UpdateCleanupPolicy.getTimer());

        //Set up nodetool flush task
        String timerVal = config.getFlushInterval();  //e.g. hour=0 or daily=10)
        if (timerVal != null && !timerVal.isEmpty() ) {
            scheduler.addTask(FlushTask.JOBNAME, FlushTask.class, FlushTask.getTimer(config));
            logger.info("Added nodetool flush task.");
        }
    }

    public InstanceIdentity getId()
    {
        return id;
    }

    public PriamScheduler getScheduler()
    {
        return scheduler;
    }

    public IConfiguration getConfiguration()
    {
        return config;
    }

}