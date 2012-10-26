package com.netflix.priam.scheduler;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.Trigger;

import java.text.ParseException;

/**
 * Scheduling class to schedule Priam tasks. Uses Quartz scheduler
 */
@Singleton
public class PriamScheduler {
    private final Scheduler scheduler;
    private final GuiceJobFactory jobFactory;

    @Inject
    public PriamScheduler(GuiceJobFactory jobFactory) {
        try {
            this.scheduler = new StdSchedulerFactory().getScheduler();
            this.scheduler.setJobFactory(jobFactory);
            this.jobFactory = jobFactory;
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    //This method should be used to add a Task
    public void addTask(JobDetail job, Trigger trigger) throws SchedulerException{
        scheduler.scheduleJob(job,trigger);

    }

    public void runTaskNow(Class<? extends Task> taskclass) throws Exception {
        jobFactory.guice.getInstance(taskclass).execute(null);
    }

    public void deleteTask(JobDetail jobDetail) throws SchedulerException, ParseException {
        scheduler.deleteJob(jobDetail.getKey());
    }

    public final Scheduler getScheduler() {
        return scheduler;
    }

    public void shutdown() {
        try {
            scheduler.shutdown();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        try {
            scheduler.start();
        } catch (SchedulerException ex) {
            throw new RuntimeException(ex);
        }
    }

}
