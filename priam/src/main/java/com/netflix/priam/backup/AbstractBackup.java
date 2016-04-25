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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;

/**
 * Abstract Backup class for uploading files to backup location
 */
public abstract class AbstractBackup extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackup.class);
    protected final List<String> FILTER_KEYSPACE = Arrays.asList("OpsCenter");
    protected final Map<String, List<String>> FILTER_COLUMN_FAMILY = ImmutableMap.of("system", Arrays.asList("local", "peers", "LocationInfo")); 
    protected final Provider<AbstractBackupPath> pathFactory;
    protected IBackupFileSystem fs;

    @Inject
    public AbstractBackup(IConfiguration config, @Named("backup") IFileSystemContext backupFileSystemCtx,Provider<AbstractBackupPath> pathFactory)
    {
        super(config);
        this.pathFactory = pathFactory;
        this.fs = backupFileSystemCtx.getFileStrategy(config);
    }
    
    /*
     * A means to override the type of backup strategy chosen via BackupFileSystemContext
     */
    protected void setFileSystem(IBackupFileSystem fs) {
    	this.fs = fs;
    }   
    
    /*
     * search for "1:* alphanumeric chars including special chars""literal period"" 1:* alphanumeric chars  including special chars"
     * @param input string
     * @return true if input string matches search pattern; otherwise, false
     */
    protected boolean isValidCFFilterFormat(String cfFilter) {
    	Pattern p = Pattern.compile(".\\..");
    	Matcher m = p.matcher(cfFilter);
    	return m.find();
    }
   
    /**
     * Upload files in the specified dir. Does not delete the file in case of
     * error
     * 
     * @param parent
     *            Parent dir
     * @param type
     *            Type of file (META, SST, SNAP etc)
     * @return
     * @throws Exception
     */
    protected List<AbstractBackupPath> upload(File parent, final BackupFileType type) throws Exception
    {
        final List<AbstractBackupPath> bps = Lists.newArrayList();
        for (final File file : parent.listFiles())
        {

            try
            {
                logger.info(String.format("Uploading file %s within CF %s for backup", file.getCanonicalFile(), parent.getAbsolutePath()));
                AbstractBackupPath abp = new RetryableCallable<AbstractBackupPath>(3, RetryableCallable.DEFAULT_WAIT_TIME)
                {
                    public AbstractBackupPath retriableCall() throws Exception
                    {
                        final AbstractBackupPath bp = pathFactory.get();
                        bp.parseLocal(file, type);
                        upload(bp); 
                        file.delete();
                        return bp;
                    }
                }.call();

                if(abp != null)
                    bps.add(abp);
                
                logger.info(String.format("Uploaded file %s within CF %s for backup", file.getCanonicalFile(), parent.getAbsolutePath()));
                addToRemotePath(abp.getRemotePath());
            }
            catch(Exception e)
            {
                logger.error(String.format("Failed to upload local file %s within CF %s. Ignoring to continue with rest of backup.", file.getCanonicalFile(), parent.getAbsolutePath()), e);
            }
        }
        return bps;
    }

    /**
     * Upload specified file (RandomAccessFile) with retries
     */
    protected void upload(final AbstractBackupPath bp) throws Exception
    {
        new RetryableCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
            	java.io.InputStream is = null;
            	try {
                	is = bp.localReader();
                	if (is == null) {
                		throw new NullPointerException("Unable to get handle on file: " + bp.fileName);
                	}
                    fs.upload(bp, is);
                    return null;            		
            	} catch (Exception e) {
            		logger.error(String.format("Exception uploading local file %S,  releasing handle, and will retry."
            				, bp.backupFile.getCanonicalFile()));
            		if (is != null) {
                		is.close();            			
            		}
            		throw e;
            	}

            }
        }.call();
    }

    /**
     * Filters unwanted keyspaces and column families
     */
    public boolean isValidBackupDir(File keyspaceDir, File columnFamilyDir, File backupDir)
    {
        if (!backupDir.isDirectory() && !backupDir.exists())
            return false;
        String keyspaceName = keyspaceDir.getName();
        if (FILTER_KEYSPACE.contains(keyspaceName)) {
        	logger.debug(keyspaceName + " is not consider a valid keyspace backup directory, will be bypass.");
            return false;        	
        }

        String columnFamilyName = columnFamilyDir.getName();

        if (FILTER_COLUMN_FAMILY.containsKey(keyspaceName) && FILTER_COLUMN_FAMILY.get(keyspaceName).contains(columnFamilyName)) {
        	logger.debug(columnFamilyName + " is not consider a valid CF backup directory, will be bypass.");
            return false;        	
        }

        return true;
    }
    
    /**
     * Adds Remote path to the list of Remote Paths
     */
    protected abstract void addToRemotePath(String remotePath);
    
    public enum DIRECTORYTYPE {
    	KEYSPACE, CF
    }
}
