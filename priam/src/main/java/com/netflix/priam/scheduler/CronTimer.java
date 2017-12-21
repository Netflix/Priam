/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.scheduler;

import java.text.ParseException;

import org.quartz.*;

/**
 * Runs jobs at the specified absolute time and frequency
 */
public class CronTimer implements TaskTimer
{
    private String cronExpression;

    public enum DayOfWeek
    {
        SUN, MON, TUE, WED, THU, FRI, SAT
    }

    /**
     * Hourly cron.
     */
    public CronTimer(int minute, int sec)
    {
        cronExpression = sec + " " + minute + " * * * ?";
    }

    /**
     * Daily Cron
     */
    public CronTimer(int hour, int minute, int sec)
    {
        cronExpression = sec + " " + minute + " " + hour + " * * ?";
    }

    /**
     * Weekly cron jobs
     */
    public CronTimer(DayOfWeek dayofweek, int hour, int minute, int sec)
    {
        cronExpression = sec + " " + minute + " " + hour + " * * " + dayofweek;
    }

    /**
     * Cron Expression.
     */
    public CronTimer(String expression)
    {
        this.cronExpression = expression;
    }

    public Trigger getTrigger() throws ParseException
    {
        return TriggerBuilder.newTrigger().withIdentity("CronTrigger", Scheduler.DEFAULT_GROUP).withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();

    }
}
