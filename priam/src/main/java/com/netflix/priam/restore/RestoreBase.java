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
	
	protected void fetchMetaFile(List<AbstractBackupPath> out, Date startTime, Date endTime) {
		String prefix = getRestorePrefix();
        logger.info("Looking for meta file here:  " + prefix);
        Iterator<AbstractBackupPath> backupfiles = fs.list(prefix, startTime, endTime);
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

	@Override
	public void execute() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getName() {
		return this.jobName;
	}	

}