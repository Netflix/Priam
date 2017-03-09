package com.netflix.priam.scheduler;

/**
 * Created by aagrawal on 3/8/17.
 */
public enum SchedulerType {
    TIME("TIME"),CRON("CRON");

    private final String schedulerType;

    SchedulerType(String schedulerType)
    {
        this.schedulerType = schedulerType.toUpperCase();
    }

    public String getSchedulerType()
    {
        return schedulerType;
    }

    //Helper method to find the scheduler type - case insensitive as user may
    //put value which are not right case.
    public static SchedulerType valueOfIgnoreCase(String schedulerType) {
        return SchedulerType.valueOf(schedulerType.toUpperCase());
    }

    public static SchedulerType lookup(String schedulerType) {
        return valueOfIgnoreCase(schedulerType);
    }

}
