package com.netflix.priam.aws;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Updates the cleanup policy for the bucket
 *
 */
@Singleton
public class UpdateCleanupPolicy extends Task
{
    public static final String JOBNAME = "UpdateCleanupPolicy";
    private final IBackupFileSystem fs;
    
    @Inject
    public UpdateCleanupPolicy(IConfiguration config, IBackupFileSystem fs)
    {
        super(config);
        this.fs = fs;
     
    }

    @Override
    public void execute() throws Exception
    {
        //Set cleanup policy of retention is specified
        new RetryableCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
                if( config.getBackupRetentionDays() > 0)
                    fs.cleanup();
                return null;
            }
        }.call();
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }

}
