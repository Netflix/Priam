package com.netflix.priam.scheduler;

import org.quartz.Job;
import org.quartz.spi.JobFactory;

import java.util.function.Function;

public interface PriamJobFactory extends JobFactory {
    Job newJob(Class<? extends Task> jobClass);
}
