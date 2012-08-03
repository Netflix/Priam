package com.netflix.priam.scheduler;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import com.google.inject.Inject;
import com.google.inject.Injector;

public class GuiceJobFactory implements JobFactory
{
    public final Injector guice;

    @Inject
    public GuiceJobFactory(Injector guice)
    {
        this.guice = guice;
    }

    @Override
    public Job newJob(TriggerFiredBundle bundle) throws SchedulerException
    {
        JobDetail jobDetail = bundle.getJobDetail();
        Class<?> jobClass = jobDetail.getJobClass();
        Job job = (Job) guice.getInstance(jobClass);
        guice.injectMembers(job);
        return job;
    }
}
