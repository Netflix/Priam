package com.netflix.priam.aws;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 * Updates the cleanup policy for the bucket
 */
@Singleton
public class UpdateCleanupPolicy extends Task {
    public static final String JOBNAME = "UpdateCleanupPolicy";
    private final IBackupFileSystem fs;

    @Inject
    public UpdateCleanupPolicy(IBackupFileSystem fs) {
        super();
        this.fs = fs;

    }

    @Override
    public void execute() throws Exception {
        // Set cleanup policy of retention is specified
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                fs.cleanup();
                return null;
            }
        }.call();
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME);
    }

    public static JobDetail getJobDetail(){
        JobDetail jobDetail = JobBuilder.newJob(Restore.class)
                .withIdentity("priam-scheduler", "update-cleanup-policy")
                .build();
        return jobDetail;
    }

    public static Trigger getTrigger(){
        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("priam-scheduler", "update-cleanup-policy-trigger")
                .startNow()
                .build();
        return trigger;
    }

}
