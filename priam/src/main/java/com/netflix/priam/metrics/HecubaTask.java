package com.netflix.priam.metrics;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.JMXConnectionException;
/**
 * In greek mythology Hecuba is the Priam's wife and the mother of cassandra. 
 * Therefore monitoring/taking care of cassandra is Hecuba's task!
 *
 */
@Singleton
public class HecubaTask extends Task {

	public static final String JOBNAME = "HECUBA_THREAD";
	public static final long INTERVAL = 60L*1000;
	public static final int HECUBA_INITIAL_WAIT_TIME_IN_SECS = 30; 
    private static final Logger logger = LoggerFactory.getLogger(HecubaTask.class);
    
    private MetricsSender sender;
    private MetricsCollector collector;
	
    // used to disable/enable metrics via REST call
    private boolean metricsEnabled;
    
	
	@Inject
    public HecubaTask(IConfiguration config, MetricsSender sender) throws JMXConnectionException {
		super(config);
		this.collector = new MetricsCollector(config);
		this.sender = sender;
		this.metricsEnabled = config.isMetricsEnabled();
		logger.info("HecubaTask was instantiated");
	}
	
	
	@Override
	public void execute() throws Exception {
		if (!metricsEnabled) {
			logger.debug("Collecting metrics is disabled!");
			return;
		}
		List<MetricDatum> datums = collector.collectMetrics();
		sender.sendMetrics(datums);
		logger.debug("Executed Hecuba task ...");
	}

	@Override
	public String getName() {
		return JOBNAME;
	}
	
	public static TaskTimer getTimer() {
		return new SimpleTimer(JOBNAME, INTERVAL);
	}
	
	
	public void setMetricsEnabled(boolean metricsEnabled) {
		this.metricsEnabled = metricsEnabled;
	}
	
	public boolean isMetricsEnabled() {
		return metricsEnabled;
	}

}
