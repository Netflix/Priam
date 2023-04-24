package com.netflix.priam.scheduler;

import com.google.inject.ImplementedBy;
import org.quartz.Job;
import org.quartz.spi.JobFactory;

@ImplementedBy(GuiceJobFactory.class)
public interface PriamJobFactory extends JobFactory {
    Job newJob(Class<? extends Task> jobClass);
}
