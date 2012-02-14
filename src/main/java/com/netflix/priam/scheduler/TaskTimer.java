package com.netflix.priam.scheduler;

import java.text.ParseException;

import org.quartz.Trigger;

/**
 * Interface to represent time/interval 
 */
public interface TaskTimer
{
    public Trigger getTrigger() throws ParseException;
}
