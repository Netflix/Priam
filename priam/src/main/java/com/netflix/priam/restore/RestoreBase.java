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

import java.util.*;
import java.io.IOException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.AbstractRestore;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.utils.Sleeper;

/*
 * Provides common functionality applicable to all restore strategies
 */
public class RestoreBase extends AbstractRestore {
	private static final Logger logger = LoggerFactory.getLogger(RestoreBase.class);
	
	private final String jobName;
	
	protected RestoreBase(IConfiguration config, IBackupFileSystem fs, String jobName, Sleeper sleeper) {
		super(config, fs, jobName, sleeper);
		
		this.jobName = jobName;
	}
	
    protected static boolean isRestoreEnabled(IConfiguration conf)
    {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getRestoreSnapshot());
        boolean isBackedupRac = (CollectionUtils.isEmpty(conf.getBackupRacs()) || conf.getBackupRacs().contains(conf.getRac()));
        return (isRestoreMode && isBackedupRac);
    }	
	
	protected void stopCassProcess(ICassandraProcess cassProcess) throws IOException {
        if (config.getRestoreKeySpaces().size() == 0)
            cassProcess.stop();
		
	}
	
	/*
	 * Fetches all files of type META.
	 */
	protected void fetchMetaFile(String restorePrefix, List<AbstractBackupPath> out, Date startTime, Date endTime) {
        logger.info("Looking for meta file here:  " + restorePrefix);
        Iterator<AbstractBackupPath> backupfiles = fs.list(restorePrefix, startTime, endTime);
        while (backupfiles.hasNext())
        {
            AbstractBackupPath path = backupfiles.next();
            if (path.getType() == BackupFileType.META)
                out.add(path);
        }
        
        return;
        		
	}
	
	protected String getRestorePrefix() {
        String prefix = "";

        if (StringUtils.isNotBlank(config.getRestorePrefix()))
            prefix = config.getRestorePrefix();
        else
            prefix = config.getBackupPrefix();
        
        return prefix;
		
	}
	
	/*
	 * Fetches meta.json used to store snapshots metadata.
	 */
	protected void fetchSnapshotMetaFile(String restorePrefix, List<AbstractBackupPath> out, Date startTime, Date endTime ) {
		logger.debug("Looking for snapshot meta file within restore prefix: " + restorePrefix);
		
        Iterator<AbstractBackupPath> backupfiles = fs.list(restorePrefix, startTime, endTime);
        if (!backupfiles.hasNext()) {
        	throw new IllegalStateException("meta.json not found, restore prefix: " + restorePrefix);
        }

        while (backupfiles.hasNext())
        {
            AbstractBackupPath path = backupfiles.next();
            if (path.getType() == BackupFileType.META)
            	//Since there are now meta file for incrementals as well as snapshot, we need to find the correct one (i.e. the snapshot meta file (meta.json))
            	if (path.getFileName().equalsIgnoreCase("meta.json")) {
            		out.add(path);            		
            	}
        }
        
        return;
	}

	@Override
	public void execute() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getName() {
		return this.jobName;
	}	

}