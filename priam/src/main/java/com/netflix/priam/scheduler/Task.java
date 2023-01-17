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

import com.netflix.priam.config.IConfiguration;
import java.util.concurrent.atomic.AtomicInteger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task class that should be implemented by all cron tasks. Jobconf will contain any instance
 * specific data
 *
 * <p>NOTE: Constructor must not throw any exception. This will cause Quartz to set the job to
 * failure
 */
public abstract class Task implements Job {
    public STATE status = STATE.DONE;

    public enum STATE {
        ERROR,
        RUNNING,
        DONE,
        NOT_APPLICABLE
    }

    protected final IConfiguration config;

    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();

    protected Task(IConfiguration config) {
        this.config = config;
    }

    /** This method has to be implemented and cannot throw any exception. */
    public void initialize() throws ExecutionException {
        // nothing to initialize
    }

    public abstract void execute() throws Exception;

    /** Main method to execute a task */
    public void execute(JobExecutionContext context) throws JobExecutionException {
        executions.incrementAndGet();
        try {
            if (status == STATE.RUNNING) return;
            status = STATE.RUNNING;
            execute();

        } catch (Throwable e) {
            status = STATE.ERROR;
            logger.error("Could not execute the task: {} because of {}", getName(), e.getMessage());
            e.printStackTrace();
            errors.incrementAndGet();
        }
        if (status != STATE.ERROR) status = STATE.DONE;
    }

    public STATE state() {
        return status;
    }

    public int getErrorCount() {
        return errors.get();
    }

    public int getExecutionCount() {
        return executions.get();
    }

    public abstract String getName();
}
