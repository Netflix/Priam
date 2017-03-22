package com.netflix.priam.scheduler;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by aagrawal on 3/8/17.
 */
public enum SchedulerType {
    HOUR("HOUR"),CRON("CRON");

    private static final Logger logger = LoggerFactory.getLogger(CronTimer.class);
    private final String schedulerType;

    SchedulerType(String schedulerType)
    {
        this.schedulerType = schedulerType.toUpperCase();
    }

    /*
     * Helper method to find the scheduler type - case insensitive as user may put value which are not right case.
     * This returns the ScheulerType if one is found. Refer to table below to understand the use-case.
     *
     * SchedulerTypeValue|acceptNullorEmpty|acceptIllegalValue|Result
     * Valid value       |NA               |NA                |SchedulerType
     * Empty string      |True             |NA                |NULL
     * NULL              |True             |NA                |NULL
     * Empty string      |False            |NA                |UnsupportedTypeException
     * NULL              |False            |NA                |UnsupportedTypeException
     * Illegal value     |NA               |True              |NULL
     * Illegal value     |NA               |False             |UnsupportedTypeException
     */

    public static SchedulerType lookup(String schedulerType, boolean acceptNullOrEmpty, boolean acceptIllegalValue) throws UnsupportedTypeException{
        if (StringUtils.isEmpty(schedulerType))
            if(acceptNullOrEmpty)
                return null;
            else
            {
                String message = String.format("%s is not a supported SchedulerType. Supported values are %s", schedulerType, getSupportedValues());
                logger.error(message);
                throw new UnsupportedTypeException(message);
            }

        try{
            return SchedulerType.valueOf(schedulerType.toUpperCase());
        }catch (IllegalArgumentException ex)
        {
            String message = String.format("%s is not a supported SchedulerType. Supported values are %s", schedulerType, getSupportedValues());

            if (acceptIllegalValue) {
                message = message + ". Since acceptIllegalValue is set to True, returning NULL instead.";
                logger.error(message);
                return null;
            }

            logger.error(message);
            throw new UnsupportedTypeException(message, ex);
        }
    }

    private static String getSupportedValues()
    {
        StringBuffer supportedValues = new StringBuffer();
        boolean first = true;
        for (SchedulerType type : SchedulerType.values()) {
            if (!first)
                supportedValues.append(",");
            supportedValues.append(type);
            first = false;
        }

        return supportedValues.toString();
    }

    public static SchedulerType lookup(String schedulerType) throws UnsupportedTypeException
    {
        return lookup(schedulerType, false, false);
    }

    public String getSchedulerType()
    {
        return schedulerType;
    }

}
