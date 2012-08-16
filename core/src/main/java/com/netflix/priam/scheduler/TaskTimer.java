package com.netflix.priam.scheduler;

import org.quartz.Trigger;

import java.text.ParseException;

/**
 * Interface to represent time/interval
 */
public interface TaskTimer {
    public Trigger getTrigger() throws ParseException;
}
