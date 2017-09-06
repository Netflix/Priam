/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.cluster.management;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.merics.NodeToolFlushMeasurement;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import com.netflix.priam.utils.JMXConnectorMgr;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by vinhn on 10/13/16.
 */
@Singleton
public class FlushTask extends Task {
    public static final String JOBNAME = "FlushTask";
    private static final Logger logger = LoggerFactory.getLogger(FlushTask.class);
    private IMetricPublisher metricPublisher;
    private IMeasurement measurement;

    @Inject
    public FlushTask(IConfiguration config, IMetricPublisher metricPublisher) {
        super(config);
        this.metricPublisher = metricPublisher;
        measurement = new NodeToolFlushMeasurement();
    }

    @Override
    public void execute() throws Exception {
        JMXConnectorMgr connMgr = null;
        List<String> flushed = null;

        try {
            connMgr = new JMXConnectorMgr(config);
            IClusterManagement flush = new Flush(this.config, connMgr);
            flushed = flush.execute();
            measurement.incrementSuccessCnt(1);
            this.metricPublisher.publish(measurement); //signal that there was a success

        } catch (Exception e) {
            measurement.incrementFailureCnt(1);
            this.metricPublisher.publish(measurement); //signal that there was a failure
            throw new RuntimeException("Exception during flush.", e);

        } finally {
            if (connMgr != null)
                connMgr.close();  //resource cleanup
        }
        if (flushed.isEmpty()) {
            logger.warn("Flush task completed successfully but no keyspaces were flushed.");
        } else {
            for (String ks : flushed) {
                logger.info("Flushed keyspace: " + ks);
            }
        }

    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    /**
     * Timer to be used for flush interval.
     * @param config {@link IConfiguration} to get configuration details from priam.
     * @return the timer to be used for flush interval.
     * <p>
     * If {@link IConfiguration#getFlushSchedulerType()} is {@link com.netflix.priam.scheduler.SchedulerType#HOUR} then it expects {@link IConfiguration#getFlushInterval()} in the format of hour=x or daily=x
     * <p>
     * If {@link IConfiguration#getFlushSchedulerType()} is {@link com.netflix.priam.scheduler.SchedulerType#CRON} then it expects a valid CRON expression from {@link IConfiguration#getFlushCronExpression()}
     */
    public static TaskTimer getTimer(IConfiguration config) throws Exception {

        CronTimer cronTimer = null;
        switch (config.getFlushSchedulerType()) {
            case HOUR:
                String timerVal = config.getFlushInterval();  //e.g. hour=0 or daily=10
                if (timerVal == null)
                    return null;
                String s[] = timerVal.split("=");
                if (s.length != 2) {
                    throw new IllegalArgumentException("Flush interval format is invalid.  Expecting name=value, received: " + timerVal);
                }
                String name = s[0].toUpperCase();
                Integer time = new Integer(s[1]);
                switch (name) {
                    case "HOUR":
                        cronTimer = new CronTimer(JOBNAME, time, 0); //minute, sec after each hour
                        break;
                    case "DAILY":
                        cronTimer = new CronTimer(JOBNAME, time, 0, 0); //hour, minute, sec to run on a daily basis
                        break;
                    default:
                        throw new UnsupportedTypeException("Flush interval type is invalid.  Expecting \"hour, daily\", received: " + name);
                }

                break;
            case CRON:
                String cronExpression = config.getFlushCronExpression();

                if (StringUtils.isEmpty(cronExpression)) {
                    logger.info("Skipping flush as flush cron is not set.");
                } else {
                    if (!CronExpression.isValidExpression(cronExpression))
                        throw new Exception("Invalid CRON expression: " + cronExpression +
                                ". Please remove cron expression if you wish to disable flush else fix the CRON expression and try again!");

                    cronTimer = new CronTimer(JOBNAME, cronExpression);
                    logger.info(String.format("Starting flush with CRON expression %s", cronTimer.getCronExpression()));
                }
                break;
        }
        return cronTimer;
    }
}
