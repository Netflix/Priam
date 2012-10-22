package com.netflix.priam.scheduler;

import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.Trigger;
import org.quartz.SimpleScheduleBuilder;

import java.text.ParseException;
import java.util.Date;

/**
 * SimpleTimer allows jobs to run starting from specified time occurring at
 * regular frequency's. Frequency of the execution timestamp since epoch.
 */
public class SimpleTimer implements TaskTimer {
    private SimpleTrigger trigger;

    public SimpleTimer(String name, long interval) {
        this.trigger = (SimpleTrigger) TriggerBuilder.newTrigger().forJob(name).withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(interval).repeatForever().withMisfireHandlingInstructionFireNow()).build();
    }

    /**
     * Run once at given time...
     */
    public SimpleTimer(String name, String group, long startTime) {
        this.trigger = (SimpleTrigger) TriggerBuilder.newTrigger().forJob(name,group).withSchedule(SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow()).startAt(new Date(startTime)).build();
    }

    /**
     * Run immediatly and dont do that again.
     */
    public SimpleTimer(String name) {
        this.trigger = (SimpleTrigger) TriggerBuilder.newTrigger().forJob(name).withSchedule(SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow()).startNow().build();
    }

    public Trigger getTrigger() throws ParseException {
        return trigger;
    }
}
