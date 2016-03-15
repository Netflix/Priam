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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.util.io.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackup.DIRECTORYTYPE;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.cryptography.IFileCryptography;
import com.netflix.priam.scheduler.NamedThreadPoolExecutor;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.FifoQueue;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;

/*
 * A means to perform a restore.  This class contains the following characteristics:
 * - It is agnostic to the source type of the restore, this is determine by the injected IBackupFileSystem.
 * - This class can be scheduled, i.e. it is a "Task".
 * - When this class is executed, it uses its own thread pool to execute the restores.
 */
public abstract class AbstractRestore extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRestore.class);
    private static final String SYSTEM_KEYSPACE = "system";
    // keeps track of the last few download which was executed.
    // TODO fix the magic number of 1000 => the idea of 80% of 1000 files limit per s3 query
    protected static final FifoQueue<AbstractBackupPath> tracker = new FifoQueue<AbstractBackupPath>(800);
    private AtomicInteger count = new AtomicInteger();
    protected final IBackupFileSystem fs;
    
    protected final IConfiguration config;
    protected final ThreadPoolExecutor executor;
    private final Map<String, Object> restoreCFFilter = new HashMap<String, Object>();
	private final Map<String, Object> restoreKeyspaceFilter  = new HashMap<String, Object>();

    public static BigInteger restoreToken;
    
    protected final Sleeper sleeper;
    
    public AbstractRestore(IConfiguration config, IBackupFileSystem fs, String name, Sleeper sleeper)
    {
        super(config);
        this.config = config;
        this.fs = fs;
        this.sleeper = sleeper;
        executor = new NamedThreadPoolExecutor(config.getMaxBackupDownloadThreads(), name);
        executor.allowCoreThreadTimeOut(true);
        
        init();
    }
    
    private void init() {
    	populateRestoreFilters();
    }
    
    private void populateRestoreFilters() {
    	String keyspaceFilters = this.config.getRestoreKeyspaceFilter();
    	if (keyspaceFilters == null || keyspaceFilters.isEmpty()) {
    		
    		logger.info("No keyspace filter set for restore.");
    		
    	} else {

        	String[] keyspaces = keyspaceFilters.split(",");
        	for (int i=0; i < keyspaces.length; i++ ) {
        		logger.info("Adding restore keyspace filter: " + keyspaces[i]);
        		this.restoreKeyspaceFilter.put(keyspaces[i], null);
        	}    		
    		
    	}
    	
    	String cfFilters = this.config.getRestoreCFFilter();
    	if (cfFilters == null || cfFilters.isEmpty()) {
    		
    		logger.info("No column family filter set for restore.");
    		
    	} else {

        	String[] cf = cfFilters.split(",");
        	for (int i=0; i < cf.length; i++) {
        		if (isValidCFFilterFormat(cf[i])) {
        			logger.info("Adding restore CF filter: " + cf[i]);
            		this.restoreCFFilter.put(cf[i], null);        			
        		} else {
        			throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + cf[i]);
        		}
        	}    		
    		
    	}
    }
    
    /*
     * search for "1:* alphanumeric chars including special chars""literal period"" 1:* alphanumeric chars  including special chars"
     * @param input string
     * @return true if input string matches search pattern; otherwise, false
     */
    private boolean isValidCFFilterFormat(String cfFilter) {
    	Pattern p = Pattern.compile(".\\..");
    	Matcher m = p.matcher(cfFilter);
    	return m.find();
    }

    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType bkupFileType) throws Exception
    {
        while (fsIterator.hasNext())
        {
            AbstractBackupPath temp = fsIterator.next();
            if (temp.type == BackupFileType.SST && tracker.contains(temp))
                continue;
            
            if ( isFiltered(DIRECTORYTYPE.KEYSPACE, temp.getKeyspace())  ) { //keyspace filtered?
            	logger.info("Bypassing restoring file \"" + temp.newRestoreFile() + "\" as its keyspace: \"" + temp.getKeyspace() + "\" is part of the filter list");
            	continue;
            }
            
            if (isFiltered(DIRECTORYTYPE.CF, temp.getKeyspace(), temp.getColumnFamily())) {
            	logger.info("Bypassing restoring file \"" + temp.newRestoreFile() + "\" as it is part of the keyspace.columnfamily filter list.  Its keyspace:cf is: "
            			+ temp.getKeyspace() + ":" + temp.getColumnFamily());
            	continue;
            }
            
            if (temp.getType() == bkupFileType)
            {   
            	File localFileHandler = temp.newRestoreFile();
            	logger.debug("Created local file name: " + localFileHandler.getAbsolutePath() + File.pathSeparator + localFileHandler.getName());
                download(temp, localFileHandler);
            }   
        }
        waitToComplete();
    }
    
    /*
     * @param keyspace or columnfamily directory type.
     * @return true if directory should be filter from processing; otherwise, false.
     */
    private boolean isFiltered(DIRECTORYTYPE directoryType, String...args) {
    	if (directoryType.equals(DIRECTORYTYPE.CF)) {
    		String keyspaceName = args[0];
    		String cfName = args[1];
    		if (this.restoreKeyspaceFilter.containsKey(keyspaceName)) { //account for keyspace which we want to filter
    			return true;
    		} else {
    			StringBuffer strBuf = new StringBuffer();
    			strBuf.append(keyspaceName);
    			strBuf.append('.');
    			strBuf.append(cfName);
            	return this.restoreCFFilter.containsKey(strBuf.toString());    			
    		}
        	
    	} else if (directoryType.equals(DIRECTORYTYPE.KEYSPACE)) {
    		return this.restoreKeyspaceFilter.containsKey(args[0]);
    		
    	} else {
    		throw new UnsupportedOperationException("Directory type not supported.  Invalid input: " + directoryType.name());
    	}

    }
    
    public class BoundedList<E> extends LinkedList<E> {

        private final int limit;

        public BoundedList(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean add(E o) {
            super.add(o);
            while (size() > limit) 
            { 
            	super.remove(); 
            }
            return true;
        }
    }
    
    protected void download(Iterator<AbstractBackupPath> fsIterator, BackupFileType filter, int lastN) throws Exception
    {
    	if (fsIterator == null)
    		return;
    	
    	BoundedList bl = new BoundedList(lastN);
    	while (fsIterator.hasNext())
    	{
    		AbstractBackupPath temp = fsIterator.next();
            if (temp.type == BackupFileType.SST && tracker.contains(temp))
                continue;
            
            if (temp.getType() == filter)
            {   
            	bl.add(temp);
            }  
    	}
    	
    	download(bl.iterator(), filter);
    }

    /**
     * Download to specific location
     */
    public void download(final AbstractBackupPath path, final File restoreLocation) throws Exception
    {
        if (config.getRestoreKeySpaces().size() != 0 && (!config.getRestoreKeySpaces().contains(path.keyspace) || path.keyspace.equals(SYSTEM_KEYSPACE)))
            return;
        count.incrementAndGet();
        executor.submit(new RetryableCallable<Integer>()
        {
            @Override
            public Integer retriableCall() throws Exception
            {
                logger.info("Downloading file: " + path.getRemotePath() + " to: " + restoreLocation.getAbsolutePath());
                fs.download(path, new FileOutputStream(restoreLocation),restoreLocation.getAbsolutePath());
                tracker.adjustAndAdd(path);
                // TODO: fix me -> if there is exception the why hang?
                
                
                logger.info("Completed download of file: " + path.getRemotePath() + " to: " + restoreLocation.getAbsolutePath());
                return count.decrementAndGet();
            }
        });
    }
    
    
    /*
     * An overloaded download where it will not only download the object but decrypt and uncompress the
     * 
     *  @param path - path of object to download from source.
     *  @param out - handle to the FINAL destination stream.
     *  Note: if this behavior is successfull, it will close the output stream.
     *  
     *  @param temp - file handlle to the downloaded file (i.e. file not decrypted yet).  To ensure widest compatability with various encryption/decryption
     *  
     *  Note: the temp file will be removed on successful processing.
     *  
     *  algorithm, we download the file completely to disk and then decrypt.  This is a temporary file and will be deleted once this behavior completes.
     *  @param fileCrypotography - the implemented cryptography algorithm use to decrypt.
     *  @param passPhrase - if necessary, the pass phrase use by the cryptography algorithm to decrypt.
     */
    public void download(final AbstractBackupPath path, final OutputStream finalDestination,  final File tempFile
		, final IFileCryptography fileCryptography
		, final char[] passPhrase
		, final ICompression compress) {
    	
        if (config.getRestoreKeySpaces().size() != 0 && (!config.getRestoreKeySpaces().contains(path.keyspace) || path.keyspace.equals(SYSTEM_KEYSPACE)))
            return;
        
        count.incrementAndGet();
        executor.submit(new RetryableCallable<Integer>() {
        	
        	@Override
        	public Integer retriableCall() throws Exception {
        	
            	//== download object from source bucket
                try {
                	
                    logger.info("Downloading file from: " + path.getRemotePath() + " to: " + tempFile.getAbsolutePath());
                    fs.download(path, new FileOutputStream(tempFile),tempFile.getAbsolutePath());                    
                    tracker.adjustAndAdd(path);
                    logger.info("Completed downloading file from: " + path.getRemotePath() + " to: " + tempFile.getAbsolutePath());
                    
                	
                } catch (Exception ex) {
                	//This behavior is retryable; therefore, lets get to a clean state before each retry.
                	if (tempFile.exists()) {
                		tempFile.createNewFile();
                	}                	
                	
                	throw new Exception("Exception downloading file from: " + path.getRemotePath() + " to: " + tempFile.getAbsolutePath(), ex);
                } 
                
                //== object downloaded successfully from source, decrypt it.
                OutputStream fOut = null;  //destination file after decryption
                File decryptedFile = new File(tempFile.getAbsolutePath() + ".decrypted");
                try {

                	InputStream in = new BufferedInputStream(new FileInputStream(tempFile.getAbsolutePath()));
                    InputStream encryptedDataInputStream = fileCryptography.decryptStream(in, passPhrase, tempFile.getAbsolutePath());
                    fOut = new BufferedOutputStream(new FileOutputStream(decryptedFile));
                    Streams.pipeAll(encryptedDataInputStream, fOut);
                    logger.info("completed decrypting file: " + tempFile.getAbsolutePath() + "to final file dest: " + decryptedFile.getAbsolutePath());                	
                	
                } catch (Exception ex) {
                	//This behavior is retryable; therefore, lets get to a clean state before each retry.
                	if (tempFile.exists()) {
                		tempFile.createNewFile();
                	}
                	
                	if (decryptedFile.exists()) {
                		decryptedFile.createNewFile();
                	}
                	                	
                	throw new Exception("Exception during decryption file:  " + decryptedFile.getAbsolutePath(), ex);

                } finally {
                	if (fOut != null ) {
                		fOut.close();
                	}
                }
                                
                //== object downloaded and decrypted successfully, now uncompress it
                logger.info("Start uncompressing file: " + decryptedFile.getAbsolutePath() + " to the FINAL destination stream");
            	FileInputStream fileIs = null;
            	InputStream is = null;
            	
            	try {

            		fileIs = new FileInputStream(decryptedFile);
	            	is = new BufferedInputStream(fileIs);
	            
	            	compress.decompressAndClose(is, finalDestination);	            		
            		
            	} catch (Exception ex) {
            		IOUtils.closeQuietly(is);                	
            		throw new Exception("Exception uncompressing file: " + decryptedFile.getAbsolutePath() + " to the FINAL destination stream");
            	}
            	
            	logger.info("Completed uncompressing file: " + decryptedFile.getAbsolutePath() + " to the FINAL destination stream " 
            			+ " current worker: " + Thread.currentThread().getName());                
            	//if here, everything was successful for this object, lets remove unneeded file(s)
            	if (tempFile.exists())
            		tempFile.delete();
            	
            	if (decryptedFile.exists()) {
            		decryptedFile.delete();  
            	}
            	
            	//Note: removal of the tempFile is responsbility of the caller as this behavior did not create it.
                
                return count.decrementAndGet();                
        		
        	}
        	
        });
    	
    }
    
    
    
    /*
     * An overloaded download where it will not only download the object but also decrypt and uncompress.
     * 
     *  @param path - path of object to download from source.
     *  @param restoreLocation - file handle to the FINAL file on disk
     *  @param temp - file handlle to the downloaded file (i.e. file not decrypted yet).  To ensure widest compatability with various encryption/decryption
     *  algorithm, we download the file completely to disk and then decrypt.  This is a temporary file and will be deleted once this behavior completes.
     *  @param fileCrypotography - the implemented cryptography algorithm use to decrypt.
     *  @param passPhrase - if necessary, the pass phrase use by the cryptography algorithm to decrypt.
     */
    public void download(final AbstractBackupPath path, final File restoreLocation, final File tempFile
    		, final IFileCryptography fileCryptography
    		, final char[] passPhrase
    		, final ICompression compress
    		) throws Exception
    {
    	
    	FileOutputStream fileOs = null;
    	BufferedOutputStream os = null;
    	try {
        	fileOs = new FileOutputStream(restoreLocation);
        	os = new BufferedOutputStream(fileOs);
        	download(path, os, tempFile, fileCryptography, passPhrase, compress);
    	} catch (Exception e) {
    		fileOs.close();
    		throw new Exception("Exception in download of:  " + path.getFileName() + ", msg: " + e.getLocalizedMessage(), e );
    		
    	} finally {
    	
    		//Note: no need to close buffered outpust stream as it is done within the called download() behavior
    	}
		
    } 
    
    /*
     * A means to wait until until all threads have completed.  It blocks calling thread
     * until all tasks (ala counter "count" is 0) are completed.
     */
    protected void waitToComplete()
    {
        while (count.get() != 0)
        {
            try
            {
                sleeper.sleep(1000);
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }
    
    protected AtomicInteger getFileCount()
    {
    		return count;
    }
    
    protected void setFileCount(int cnt)
    {
    		count.set(cnt);
    }
}