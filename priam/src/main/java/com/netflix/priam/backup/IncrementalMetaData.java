package com.netflix.priam.backup;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;

public class IncrementalMetaData extends MetaData {

	private String metaFileName = null; //format meta_cf_time (e.g. 

	@Inject
	public IncrementalMetaData(IConfiguration config, Provider<AbstractBackupPath> pathFactory,@Named("backup")IFileSystemContext backupFileSystemCtx) {
		super(pathFactory, backupFileSystemCtx, config);
	}
	
	public void setMetaFileName(String name) {
		this.metaFileName = name;
	}
	
	@Override
	public File createTmpMetaFile() throws IOException{
		File metafile = null, destFile = null;
		
		if (this.metaFileName == null) {
			
	        metafile = File.createTempFile("incrementalMeta", ".json");
	        destFile = new File(metafile.getParent(), "incrementalMeta.json");
			
		} else {
	        metafile = File.createTempFile(this.metaFileName, ".json");
	        destFile = new File(metafile.getParent(), this.metaFileName + ".json");        
		}
		
        if(destFile.exists())
            destFile.delete();
        
        try {
			
        	FileUtils.moveFile(metafile, destFile);
			
		} finally {
			if (metafile != null && metafile.exists()) { //clean up resource
				FileUtils.deleteQuietly(metafile);
			}
		}
        return destFile;
    }
}
