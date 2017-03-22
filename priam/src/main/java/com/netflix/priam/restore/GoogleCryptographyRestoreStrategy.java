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
package com.netflix.priam.restore;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
import com.netflix.priam.ICredentialGeneric.KEY;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.MetaData;
import com.netflix.priam.backup.RestoreTokenSelector;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.ICredentialGeneric;
import com.netflix.priam.cryptography.pgp.PgpCredential;

@Singleton
public class GoogleCryptographyRestoreStrategy extends RestoreBase implements IRestoreStrategy {
	private static final Logger logger = LoggerFactory.getLogger(GoogleCryptographyRestoreStrategy.class);
	public static final String JOBNAME = "GOOGLECLOUDSTORAGE_RESTORE_JOB";

	@Inject
	private Provider<AbstractBackupPath> pathProvider;	
    @Inject
    private MetaData metaData;
    @Inject
    private InstanceIdentity id;   
    @Inject
    private RestoreTokenSelector tokenSelector;
    
	private ICassandraProcess cassProcess;
	private IFileCryptography fileCryptography;
	private ICompression compress;
	private ICredentialGeneric pgpCredential;	
	
	@Inject
	public GoogleCryptographyRestoreStrategy(final IConfiguration config, ICassandraProcess cassProcess, @Named("gcsencryptedbackup") IBackupFileSystem fs, Sleeper sleeper
			, @Named("filecryptoalgorithm") IFileCryptography fileCryptography
			, @Named("pgpcredential") ICredentialGeneric credential
			, ICompression compress
			) {
		
		super(config, fs, JOBNAME, sleeper);
		this.cassProcess = cassProcess;
		this.fileCryptography = fileCryptography;
		this.pgpCredential = credential;
		this.compress = compress;
	}
	
    @Override
    public void execute() throws Exception {

        if (isRestoreEnabled(config))
        {
            logger.info("Starting restore for " + config.getRestoreSnapshot()); //get the date range
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
	
	@Override
	public void restore(Date startTime, Date endTime) throws Exception {

		super.stopCassProcess(this.cassProcess);        // Stop cassandra if its running and restoring all keyspaces
		
        //== Cleanup local data
        SystemUtils.cleanupDir(super.config.getDataFileLocation(), super.config.getRestoreKeySpaces());
        
        //== Generate Json format list of all files to download. This information is derived from meta.json file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        
        fetchSnapshotMetaFile(getRestorePrefix(), metas, startTime, endTime);
        if (metas.size() == 0)
        {
        	logger.error("No snapshot meta file found, Restore Failed.");
        	assert false : "[GoogleRestoreStrategy] No snapshots meta data file found, Restore Failed.";
        	return;
        }        
        
        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Snapshot Meta file for restore " + meta.getRemotePath());

        //download, decrypt, and uncompress the metadata file
        List<AbstractBackupPath> metaFile = new ArrayList<AbstractBackupPath>();
        metaFile.add(meta);
        performDownloadType(metaFile.iterator(), BackupFileType.META);

        List<AbstractBackupPath> snapshots = metaData.toJson(meta.newRestoreFile()); //transform the decompress, decrypted meta file to json format
        
        // Download the Snapshots, decrypt, and then decompress
        performDownloadType(snapshots.iterator(), BackupFileType.SNAP);
        
        logger.info("Downloading incrementals");
        // Download incrementals (SST).
        Iterator<AbstractBackupPath> incrementals = fs.list(getRestorePrefix(), meta.getTime(), endTime);
        performDownloadType(incrementals, BackupFileType.SST);
        
        //Downloading CommitLogs
        if (config.isBackingUpCommitLogs())
        {	
        	logger.info("Delete all backuped commitlog files in " + config.getBackupCommitLogLocation());
        	SystemUtils.cleanupDir(config.getBackupCommitLogLocation(), null);
        	     
        	logger.info("Delete all commitlog files in " + config.getCommitLogLocation());
        	SystemUtils.cleanupDir(config.getCommitLogLocation(), null);
        	
        	Iterator<AbstractBackupPath> commitLogPathIterator = fs.list(getRestorePrefix(), meta.getTime(), endTime); 
        	download(commitLogPathIterator, BackupFileType.CL, config.maxCommitLogsRestore());       	
        }        		

	}
	
	/*
	 * A version of download commit logs, base on AbstractRestore.download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter, int lastN).
	 * This version does not change the original business logic, it just assumes the commit logs are encrypted and handles it.
	*/
	@Override
    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter, int lastN) throws Exception
    {
    	if (fsIterator == null)
    		return;
    	
    	BoundedList bl = new BoundedList(lastN);
    	while (fsIterator.hasNext())
    	{
    		AbstractBackupPath temp = fsIterator.next();
            if (temp.getType() == BackupFileType.SST && tracker.contains(temp))
                continue;
            
            if (temp.getType() == filter)
            {   
            	bl.add(temp);
            }  
    	}
    	
    	performDownloadType(bl.iterator(), filter);
    }		
	
	
	/*  
	 * Determines the type of download (data is encrypted or not) for the objects.
	 * 
	 * @param fsIterator - a list of objects to download.
	 * @param filer - type (e.g. SNAP, SST) of objects to download.
	 * @param decryptAndDecompress - true to decrypt the download object on disk and then decompress it..
	 */
	private void performDownloadType(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter) throws Exception {
		if ( !this.config.isEncryptBackupEnabled() ) {
			
			download(fsIterator, filter);
			
		} else {

			//download & decrypt each object
	        while (fsIterator.hasNext())
	        {
	            AbstractBackupPath temp = fsIterator.next();
	            if (temp.getType().equals(BackupFileType.SST) && tracker.contains(temp))
	                continue;
	            
	            if (temp.getType() == filter)
	            {   
	            	File localFileHandler = temp.newRestoreFile();
	            	String tempFileName = localFileHandler.getAbsolutePath() + ".tmp";
	            	File tempFileHandler = new File(tempFileName);
	            	
	            	//download from source, decrypt, and lastly uncompress
	            	
	            	//download(temp, localFileHandler, encryptedFileHandler, this.fileCryptography, this.keyCryptography.decrypt(null).toCharArray(), this.compress);
	            	String pgpPassword = new String(this.pgpCredential.getValue(KEY.PGP_PASSWORD));
	            	download(temp, localFileHandler, tempFileHandler, this.fileCryptography, pgpPassword.toCharArray(), this.compress);
	            }   
	        }
	        
	        waitToComplete(); //wait until all objects are downloaded, decrypt, and uncompress	        
	        
		}
	}	
	
	/*
	 * @return a timer used by the scheduler to determine when "this" should be run.
	 */
    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }	
    
    
    /*
     * @param pathprefix - the absolute path (including bucket name) to the object.
     * @param bucket name
     */
    public static String getSourcebucket(String pathPrefix) {

        String[] paths = pathPrefix.split(String.valueOf(S3BackupPath.PATH_SEP));
        return paths[0];    	
    	
    }

}