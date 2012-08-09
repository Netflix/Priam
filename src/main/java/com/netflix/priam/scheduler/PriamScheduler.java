package com.netflix.priam.scheduler;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;

import java.text.ParseException;

/**
 * Scheduling class to schedule Priam tasks. Uses Quartz scheduler
 */
@Singleton
public class PriamScheduler {
    private final Scheduler scheduler;
    private final GuiceJobFactory jobFactory;

    @Inject
    public PriamScheduler(SchedulerFactory factory, GuiceJobFactory jobFactory) {
        try {
            this.scheduler = factory.getScheduler();
            this.scheduler.setJobFactory(jobFactory);
            this.jobFactory = jobFactory;
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a task to the scheduler
     */
    public void addTask(String name, Class<? extends Task> taskclass, TaskTimer timer) throws SchedulerException, ParseException {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        JobDetail job = new JobDetail(name, Scheduler.DEFAULT_GROUP, taskclass);
        scheduler.scheduleJob(job, timer.getTrigger());
    }

    public void runTaskNow(Class<? extends Task> taskclass) throws Exception {
        jobFactory.guice.getInstance(taskclass).execute(null);
    }

    public void deleteTask(String name) throws SchedulerException, ParseException {
        scheduler.deleteJob(name, Scheduler.DEFAULT_GROUP);
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
