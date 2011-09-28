package com.priam.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task class that should be implemented by all cron tasks. Jobconf will contain
 * any instance specific data
 */
public abstract class Task implements Job
{
    private static final Logger logger = LoggerFactory.getLogger(Task.class);

    public static enum STATE
    {
        ERROR, RUNNING, DONE
    }

    public STATE status = STATE.DONE;

    public abstract void execute() throws Exception;

    /**
     * Main method to execute a task
     */
    public void execute(JobExecutionContext context) throws JobExecutionException
    {
        try
        {
            if (status == STATE.RUNNING)
                return;
            status = STATE.RUNNING;
            execute();
        }
        catch (Exception e)
        {
            status = STATE.ERROR;
            logger.error("Couldnt execute the task because of....", e);
        }
        catch (Throwable e)
        {
            status = STATE.ERROR;
            logger.error("Couldnt execute the task because of....", e);
        }
        if (status != STATE.ERROR)
            status = STATE.DONE;
    }

    public STATE state()
    {
        return status;
    }

    public abstract String getName();

}
