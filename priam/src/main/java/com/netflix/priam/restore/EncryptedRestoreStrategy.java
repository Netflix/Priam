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

/*
 * A strategy to restore encrypted data from a primary AWS account
 */
@Singleton
public class EncryptedRestoreStrategy extends RestoreBase implements IRestoreStrategy {
	private static final Logger logger = LoggerFactory.getLogger(EncryptedRestoreStrategy.class);
	
	public static final String JOBNAME = "CRYPTOGRAPHY_RESTORE_JOB";
	
	private ICassandraProcess cassProcess;
	private IFileCryptography fileCryptography;
	private ICompression compress;

	@Inject
	private Provider<AbstractBackupPath> pathProvider;
    @Inject
    private MetaData metaData;
    @Inject
    private InstanceIdentity id;   
    @Inject
    private RestoreTokenSelector tokenSelector;

	private ICredentialGeneric pgpCredential;	
	
	@Inject
	public EncryptedRestoreStrategy(final IConfiguration config, ICassandraProcess cassProcess, @Named("encryptedbackup") IBackupFileSystem fs, Sleeper sleeper
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
    	if (isRestoreEnabled(config)) {
    		
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
        
        fetchMetaFile(metas, startTime, endTime);
        if (metas.size() == 0)
        {
        	logger.error("No meta file found, Restore Failed.");
        	assert false : "[AwsCrossAccountCrypotographyRestoreStrategy] No snapshots meta data file found, Restore Failed.";
        	return;
        }		

        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Meta file for restore " + meta.getRemotePath());

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
        logger.info("Do we have incrementals to download? " + incrementals.hasNext());
        performDownloadType(incrementals, BackupFileType.SST);
        
        //Downloading CommitLogs
        if (config.isBackingUpCommitLogs())  //TODO: will change to isRestoringCommitLogs()
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
	 * Download, decrypt, and uncompress the list of objects.
	 * 
	 * 
	 * @param fsIterator - a list of objects to download.
	 * @param filer - type (e.g. SNAP, SST) of objects to download.
	 * @param decryptAndDecompress - true to decrypt the download object on disk and then decompress it..
	 */
	private void performDownloadType(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter) throws Exception {
		
		//download & decrypt each object
        while (fsIterator.hasNext())
        {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.getType().equals(BackupFileType.SST) && tracker.contains(temp))
                continue;
            
            if (temp.getType() == filter)
            {   
            	File finalFileHandler = temp.newRestoreFile();
            	String tempFileName = finalFileHandler.getAbsolutePath() + ".tmp";
            	File tempFileHandler = new File(tempFileName);
            	
            	//== download from source, decrypt, and lastly uncompress
            	String pgpPassword = new String(this.pgpCredential.getValue(KEY.PGP_PASSWORD));
            	download(temp, finalFileHandler, tempFileHandler, this.fileCryptography, pgpPassword.toCharArray(), this.compress);
            		                
            }   
        }
        
        waitToComplete(); //wait until all objects are downloaded, decrypt, and uncompress		
		
		
	}	
	
	/*
	 * @return a timer used by the scheduler to determine when "this" should be run.
	 */
    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }		

}