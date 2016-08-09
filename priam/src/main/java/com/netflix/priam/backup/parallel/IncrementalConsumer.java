package com.netflix.priam.backup.parallel;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackup;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.IFileSystemContext;
import com.netflix.priam.utils.RetryableCallable;

/*
 * Performs an upload of a file, with retries.
 */
public class IncrementalConsumer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(IncrementalConsumer.class);
	
	private AbstractBackupPath bp;
	private IBackupFileSystem fs;
	private BackupPostProcessingCallback<AbstractBackupPath> callback;

    /**
     * Upload files. Does not delete the file in case of
     * error.
     * 
     * @param bp - the file to upload with additional metada
     * @param incremental file - handle to the actual file to upload
     */
	public IncrementalConsumer(AbstractBackupPath bp, IBackupFileSystem fs
			, BackupPostProcessingCallback<AbstractBackupPath> callback) {
		this.bp = bp;
		this.fs = fs;
		this.callback = callback;
	}

	@Override
	/*
	 * Upload specified file, with retries logic.
	 * File will be deleted only if uploaded successfully.
	 */
	public void run() {
		
		logger.info("Consumer - about to upload file: " + this.bp.getFileName());
		
		try {
			
			new RetryableCallable<Void>() 
			{
				@Override
				public Void retriableCall() throws Exception {
					
	            	java.io.InputStream is = null;
	            	try {
	                	is = bp.localReader();
	            	}  catch (java.io.FileNotFoundException | RuntimeException e) {
	            		if (is != null) {
	                		is.close();            			
	            		}
	            		throw new java.util.concurrent.CancellationException("Someone beat me to uploading this file"
	            				+ ", no need to retry.  Most likely not needed but to be safe, checked and released handle to file if appropriate.");
	            	} 
	            	
	            	try {
	                	if (is == null) {
	                		throw new NullPointerException("Unable to get handle on file: " + bp.getFileName());
	                	}
	                    fs.upload(bp, is);
	                    return null;            		
	            	}catch (Exception e) {
	            		logger.error(String.format("Exception uploading local file %S,  releasing handle, and will retry."
	            				, bp.getFileName()));
	            		if (is != null) {
	                		is.close();            			
	            		}
	            		throw e;
	            	}
				}
			}.call();
			
			this.bp.getBackupFile().delete(); //resource cleanup
			this.callback.postProcessing(bp); //post processing
			
		} catch (Exception e) {
			if (e instanceof java.util.concurrent.CancellationException) {
				logger.debug(String.format("Failed to upload local file %s. Ignoring to continue with rest of backup.  Msg: %s"
						, this.bp.getFileName(), e.getLocalizedMessage()));
			} else {
				logger.error(String.format("Failed to upload local file %s. Ignoring to continue with rest of backup.  Msg: %s"
						, this.bp.getFileName(), e.getLocalizedMessage()));				
			}

		}
		
	}


}