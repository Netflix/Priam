package com.netflix.priam.backup;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.IMessageObserver.BACKUP_MESSAGE_TYPE;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;


//Provide this to be run as a Quart job
@Singleton
public class CommitLogBackupTask extends AbstractBackup
{
    public static String JOBNAME = "CommitLogBackup";
    
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    private final List<String> snapshotRemotePaths = new ArrayList<String>();
    static List<IMessageObserver> observers = new ArrayList<IMessageObserver>();
    private final CommitLogBackup clBackup;
    

    @Inject
    public CommitLogBackupTask(IConfiguration config, @Named("backup")IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory, 
    		                   CommitLogBackup clBackup)
    {
        super(config, fs, pathFactory);
        this.clBackup = clBackup;
    }

    
    @Override
    public void execute() throws Exception
    {
        try
        {
        	logger.debug("Checking for any archived commitlogs");
        	//double-check the permission
            if (config.isBackingUpCommitLogs())
                clBackup.upload(config.getCommitLogBackupRestoreFromDirs(), null);
        }       
        catch (Exception e)
        {
                logger.error(e.getMessage(), e);
        }
    }

  

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config)
    {
    	return new SimpleTimer(JOBNAME, 60L * 1000); //every 5 min
    }

   
    public static void addObserver(IMessageObserver observer)
    {
    		observers.add(observer);
    }
    
    public static void removeObserver(IMessageObserver observer)
    {
    		observers.remove(observer);
    }
    
    public void notifyObservers()
    {
        for(IMessageObserver observer : observers)
        {
        		if(observer != null)
        		{
        			logger.debug("Updating snapshot observers now ...");
        			observer.update(BACKUP_MESSAGE_TYPE.SNAPSHOT,snapshotRemotePaths);
        		}
        		else
        			logger.info("Observer is Null, hence can not notify ...");
        }
    }

	@Override
	protected void addToRemotePath(String remotePath) {		
		snapshotRemotePaths.add(remotePath);		
	}

}
