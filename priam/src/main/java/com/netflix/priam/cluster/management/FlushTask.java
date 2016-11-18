package com.netflix.priam.cluster.management;

import com.amazonaws.services.lambda.model.Runtime;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.IMetricPublisher;
import com.netflix.priam.merics.NoOpMetricPublisher;
import com.netflix.priam.merics.NodeToolFlushMeasurement;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.JMXConnectorMgr;
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
    public FlushTask(IConfiguration config, @Named("defaultmetricpublisher") IMetricPublisher metricPublisher) {
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
            IMeasurement measurement = new NodeToolFlushMeasurement();
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

    /*
    @return the hourly or daily time to execute the flush
     */
    public static TaskTimer getTimer(IConfiguration config) {
        String timerVal = config.getFlushInterval();  //e.g. hour=0 or daily=10
        String s[] = timerVal.split("=");
        if (s.length != 2 ){
            throw new IllegalArgumentException("Flush interval format is invalid.  Expecting name=value, received: " + timerVal);
        }
        String name = s[0];
        Integer time = new Integer(s[1]);

        if (name.equalsIgnoreCase("hour")) {
            return new CronTimer(JOBNAME, time, 0); //minute, sec after each hour
        } if (name.equalsIgnoreCase("daily")) {
            return new CronTimer(JOBNAME, time, 0 , 0); //hour, minute, sec to run on a daily basis
        } else {
            throw new IllegalArgumentException("Flush interval type is invalid.  Expecting \"hour, daily\", received: " + name);
        }
    }
}
