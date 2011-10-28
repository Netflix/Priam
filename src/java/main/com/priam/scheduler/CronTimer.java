package com.priam.scheduler;

import java.text.ParseException;

import org.quartz.CronTrigger;
import org.quartz.Scheduler;
import org.quartz.Trigger;

/**
 * Runs jobs at the specified absolute time and frequency
 * 
 * @author psadhu
 * 
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
        return new CronTrigger("CronTrigger", Scheduler.DEFAULT_GROUP, cronExpression);
    }
}
