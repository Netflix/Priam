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

import java.text.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs jobs at the specified absolute time and frequency */
public class CronTimer implements TaskTimer {
    private static final Logger logger = LoggerFactory.getLogger(CronTimer.class);
    private final String cronExpression;
    private String name;

    public enum DayOfWeek {
        SUN,
        MON,
        TUE,
        WED,
        THU,
        FRI,
        SAT
    }

    /*
     * interval in terms of minutes
     */
    public CronTimer(String name, int min) {
        this.name = name;
        cronExpression = "*" + " " + "0/" + min + " " + "* * * ?";
    }

    /** Hourly cron. */
    public CronTimer(String name, int minute, int sec) {
        this.name = name;
        cronExpression = sec + " " + minute + " 0/1 * * ?";
    }

    /** Daily Cron */
    public CronTimer(String name, int hour, int minute, int sec) {
        this.name = name;
        cronExpression = sec + " " + minute + " " + hour + " * * ?";
    }

    /** Weekly cron jobs */
    public CronTimer(String name, DayOfWeek dayofweek, int hour, int minute, int sec) {
        this.name = name;
        cronExpression = sec + " " + minute + " " + hour + " * * " + dayofweek;
    }

    /** Cron Expression. */
    public CronTimer(String expression) {
        this.cronExpression = expression;
    }

    public CronTimer(String name, String expression) {
        this.name = name;
        this.cronExpression = expression;
    }

    public Trigger getTrigger() throws ParseException {
        return TriggerBuilder.newTrigger()
                .withIdentity(name, Scheduler.DEFAULT_GROUP)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();
    }

    @Override
    public String getCronExpression() {
        return this.cronExpression;
    }

    public static CronTimer getCronTimer(final String jobName, final String cronExpression)
            throws IllegalArgumentException {
        CronTimer cronTimer = null;

        if (!StringUtils.isEmpty(cronExpression) && cronExpression.equalsIgnoreCase("-1")) {
            logger.info(
                    "Skipping {} as it is disabled via setting {} cron to -1.", jobName, jobName);
        } else {
            if (StringUtils.isEmpty(cronExpression)
                    || !CronExpression.isValidExpression(cronExpression))
                throw new IllegalArgumentException(
                        "Invalid CRON expression: "
                                + cronExpression
                                + ". Please use -1, if you wish to disable "
                                + jobName
                                + " else fix the CRON expression and try again!");

            cronTimer = new CronTimer(jobName, cronExpression);
            logger.info(
                    "Starting {} with CRON expression {}", jobName, cronTimer.getCronExpression());
        }
        return cronTimer;
    }
}
