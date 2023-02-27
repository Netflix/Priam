/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.scheduler;

import com.netflix.priam.utils.Sleeper;
import java.text.ParseException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scheduling class to schedule Priam tasks. Uses Quartz scheduler */
@Singleton
public class PriamScheduler {
    private static final Logger logger = LoggerFactory.getLogger(PriamScheduler.class);
    private final Scheduler scheduler;
    private final GuiceJobFactory jobFactory;
    private final Sleeper sleeper;

    @Inject
    public PriamScheduler(SchedulerFactory factory, GuiceJobFactory jobFactory, Sleeper sleeper) {
        try {
            this.scheduler = factory.getScheduler();
            this.scheduler.setJobFactory(jobFactory);
            this.jobFactory = jobFactory;
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        this.sleeper = sleeper;
    }

    /** Add a task to the scheduler */
    public void addTask(String name, Class<? extends Task> taskclass, TaskTimer timer)
            throws SchedulerException, ParseException {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        JobDetail job =
                JobBuilder.newJob()
                        .withIdentity(name, Scheduler.DEFAULT_GROUP)
                        .ofType(taskclass)
                        .build();
        if (timer.getCronExpression() != null && !timer.getCronExpression().isEmpty()) {
            logger.info(
                    "Scheduled task metadata.  Task name: {}" + ", cron expression: {}",
                    taskclass.getName(),
                    timer.getCronExpression());

        } else {
            logger.info("Scheduled task metadata.  Task name: {}", taskclass.getName());
        }
        scheduler.scheduleJob(job, timer.getTrigger());
    }

    /** Add a delayed task to the scheduler */
    public void addTaskWithDelay(
            final String name,
            Class<? extends Task> taskclass,
            final TaskTimer timer,
            final int delayInSeconds) {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        final JobDetail job =
                JobBuilder.newJob()
                        .withIdentity(name, Scheduler.DEFAULT_GROUP)
                        .ofType(taskclass)
                        .build();

        // we know Priam doesn't do too many new tasks, so this is probably easy/safe/simple
        new Thread(
                        () -> {
                            try {
                                sleeper.sleepQuietly(delayInSeconds * 1000L);
                                scheduler.scheduleJob(job, timer.getTrigger());
                            } catch (SchedulerException e) {
                                logger.warn(
                                        "problem occurred while scheduling a job with name {}",
                                        name,
                                        e);
                            } catch (ParseException e) {
                                logger.warn(
                                        "problem occurred while parsing a job with name {}",
                                        name,
                                        e);
                            }
                        })
                .start();
    }

    public void runTaskNow(Class<? extends Task> taskclass) throws Exception {
        jobFactory.guice.getInstance(taskclass).execute(null);
    }

    public void deleteTask(String name) throws SchedulerException {
        TriggerKey triggerKey = TriggerKey.triggerKey(name, Scheduler.DEFAULT_GROUP);

        // Check if trigger exists for the job. If there is a trigger, we want to remove those
        // trigger.
        if (scheduler.checkExists(triggerKey)) {
            logger.info("Removing triggers for the job: {}", name);
            scheduler.pauseTrigger(triggerKey);
            scheduler.unscheduleJob(triggerKey);
        }

        // Check if any job exists for the key provided. If yes, we want to delete the job.
        JobKey jobKey = JobKey.jobKey(name, Scheduler.DEFAULT_GROUP);
        if (scheduler.checkExists(jobKey)) {
            logger.info("Removing job from scheduler: {}", name);
            scheduler.deleteJob(jobKey);
        }
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
