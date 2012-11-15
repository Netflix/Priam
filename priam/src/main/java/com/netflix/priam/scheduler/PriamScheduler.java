package com.netflix.priam.scheduler;

import java.text.ParseException;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Scheduling class to schedule Priam tasks. Uses Quartz scheduler
 */
@Singleton
public class PriamScheduler
{
    private final Scheduler scheduler;
    private final GuiceJobFactory jobFactory;

    @Inject
    public PriamScheduler(SchedulerFactory factory, GuiceJobFactory jobFactory)
    {
        try
        {
            this.scheduler = factory.getScheduler();
            this.scheduler.setJobFactory(jobFactory);
            this.jobFactory = jobFactory;
        }
        catch (SchedulerException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a task to the scheduler
     */
    public void addTask(String name, Class<? extends Task> taskclass, TaskTimer timer) throws SchedulerException, ParseException
    {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        JobDetail job = new JobDetail(name, Scheduler.DEFAULT_GROUP, taskclass);
        scheduler.scheduleJob(job, timer.getTrigger());
    }

    /**
     * Add a delayed task to the scheduler
     */
    public void addTaskWithDelay(String name, Class<? extends Task> taskclass, final TaskTimer timer, final int delayInSeconds) throws SchedulerException, ParseException
    {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        final JobDetail job = new JobDetail(name, Scheduler.DEFAULT_GROUP, taskclass);
        
        //we know Priam doesn't do too many new tasks, so this is probably easy/safe/simple
        new Thread(new Runnable(){
            public void run()
            {
            		try { 
            				Thread.sleep(delayInSeconds * 1000L);
            				scheduler.scheduleJob(job, timer.getTrigger());  
            		}catch(InterruptedException ignore) {} 
            		catch (SchedulerException e) {} 
            		catch (ParseException e) {}
            }
        }).start();

    }
    
    public void runTaskNow(Class<? extends Task> taskclass) throws Exception
    {
        jobFactory.guice.getInstance(taskclass).execute(null);
    }

    public void deleteTask(String name) throws SchedulerException, ParseException
    {
        scheduler.deleteJob(name, Scheduler.DEFAULT_GROUP);
    }

    public final Scheduler getScheduler()
    {
        return scheduler;
    }

    public void shutdown()
    {
        try
        {
            scheduler.shutdown();
        }
        catch (SchedulerException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start()
    {
        try
        {
            scheduler.start();
        }
        catch (SchedulerException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
