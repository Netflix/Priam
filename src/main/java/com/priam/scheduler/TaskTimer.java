package com.priam.scheduler;

import java.text.ParseException;

import org.quartz.Trigger;

/**
 * Interface to represent time/interval 
 */
public interface TaskTimer
{
    Trigger getTrigger() throws ParseException;
}
