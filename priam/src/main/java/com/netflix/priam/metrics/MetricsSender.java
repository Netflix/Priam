package com.netflix.priam.metrics;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;

/**
 * connects to AWS cloudwatch and puts monitoring data
 */
@Singleton
public class MetricsSender {
	
	private static final Logger logger = LoggerFactory.getLogger(MetricsSender.class);
	
	// default value
	private String namespace = "CASSANDRA-METRICS";
	private AmazonCloudWatchClient cloudWatchClient;

	@Inject
	public MetricsSender(IConfiguration config, ICredential provider) {
		getNamespace(config);
		cloudWatchClient = new AmazonCloudWatchClient(provider.getAwsCredentialProvider());
		cloudWatchClient.setEndpoint(config.getCloudwatchMonitoringEndpoint());
		logger.info("Cloudwatch client with endpoint ["+ config.getCloudwatchMonitoringEndpoint() +"] successfully instantiated");
	} 
	
	
	// for testing
	protected MetricsSender(IConfiguration config, AmazonCloudWatchClient cloudWatchClient, ICredential provider) {
		getNamespace(config);
		this.cloudWatchClient = cloudWatchClient;
	}


	private void getNamespace(IConfiguration config) {
		String ns = config.getAppName();
		if (ns == null || "".equals(ns.trim())) {
			logger.warn("Using default namespace [" +namespace + "] as there's no app name configured.");
		} else {
			this.namespace  = ns;
		}
	} 
	
	/**
	 * Finally inserts metric data into cloudwatch
	 * 
	 * @param datums
	 */
	public void sendMetrics(List<MetricDatum> datums) {
		
		for (MetricDatum datum : datums) {
			logger.debug("Putting metric: [" + datum.toString()+"]");
			PutMetricDataRequest request = new PutMetricDataRequest().withMetricData(datum).withNamespace(namespace);
			cloudWatchClient.putMetricData(request);
		}
		logger.debug("Put [" + datums.size() + "] metrics into cloudwatch!");
	}
	

}
