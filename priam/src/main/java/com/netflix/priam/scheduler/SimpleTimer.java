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
import java.util.Date;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 * SimpleTimer allows jobs to run starting from specified time occurring at regular frequency's.
 * Frequency of the execution timestamp since epoch.
 */
public class SimpleTimer implements TaskTimer {
    private final Trigger trigger;

    public SimpleTimer(String name, long interval) {
        this.trigger =
                TriggerBuilder.newTrigger()
                        .withIdentity(name)
                        .withSchedule(
                                SimpleScheduleBuilder.simpleSchedule()
                                        .withIntervalInMilliseconds(interval)
                                        .repeatForever()
                                        .withMisfireHandlingInstructionFireNow())
                        .build();
        // .new SimpleTrigger(name, SimpleTrigger.REPEAT_INDEFINITELY, interval);
    }

    /** Run once at given time... */
    public SimpleTimer(String name, String group, long startTime) {
        this.trigger =
                TriggerBuilder.newTrigger()
                        .withIdentity(name, group)
                        .withSchedule(
                                SimpleScheduleBuilder.simpleSchedule()
                                        .withMisfireHandlingInstructionFireNow())
                        .startAt(new Date(startTime))
                        .build();
        // new SimpleTrigger(name, group, new Date(startTime));
    }

    /** Run immediatly and dont do that again. */
    public SimpleTimer(String name) {
        this.trigger =
                TriggerBuilder.newTrigger()
                        .withIdentity(name, Scheduler.DEFAULT_GROUP)
                        .withSchedule(
                                SimpleScheduleBuilder.simpleSchedule()
                                        .withMisfireHandlingInstructionFireNow())
                        .startNow()
                        .build();
        // new SimpleTrigger(name, Scheduler.DEFAULT_GROUP);
    }

    public Trigger getTrigger() throws ParseException {
        return trigger;
    }

    @Override
    public String getCronExpression() {
        return null;
    }
}
