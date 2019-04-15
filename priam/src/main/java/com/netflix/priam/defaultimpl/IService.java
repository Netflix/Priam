/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.priam.defaultimpl;

import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import java.text.ParseException;
import org.quartz.SchedulerException;

/**
 * This is how we create a new service in Priam. Any service we start, should implement this
 * interface so we can update the service at run-time if required.
 *
 * <p>Created by aagrawal on 3/9/19.
 */
public interface IService {
    /**
     * This method is called to schedule the service when we initialize it for first time ONLY.
     *
     * @throws Exception if there is any error while trying to schedule the service.
     */
    void scheduleService() throws Exception;

    /**
     * This method is called before we try to update the service. Use this method to do any kind of
     * maintenance operations before we change the scheduling of all the jobs in service.
     *
     * @throws Exception if there is any error in pre-hook method of service.
     */
    void updateServicePre() throws Exception;

    /**
     * This method is called after we re-schedule all the services in PriamScheduler. Use this
     * method for post hook maintenance operations after changing the scehdule of all the jobs.
     *
     * @throws Exception if there is any error in post-hook method of service.
     */
    void updateServicePost() throws Exception;

    /**
     * Schedule a given task. It will safely delete that task from scheduler before scheduling.
     *
     * @param priamScheduler Scheduler in use by Priam.
     * @param task Task that needs to be scheduled in priamScheduler
     * @param taskTimer Timer for the task
     * @throws SchedulerException If there is any error in deleting the task or scheduling a new
     *     job.
     * @throws ParseException If there is any error in parsing the taskTimer while trying to add a
     *     new job to scheduler.
     */
    default void scheduleTask(
            PriamScheduler priamScheduler, Class<? extends Task> task, TaskTimer taskTimer)
            throws SchedulerException, ParseException {
        priamScheduler.deleteTask(task.getName());
        if (taskTimer == null) return;
        priamScheduler.addTask(task.getName(), task, taskTimer);
    }

    /**
     * Update the service. This method will be called to update the service while Priam is running.
     *
     * @throws Exception if any issue while updating the service.
     */
    default void onChangeUpdateService() throws Exception {
        updateServicePre();
        scheduleService();
        updateServicePost();
    }
}
