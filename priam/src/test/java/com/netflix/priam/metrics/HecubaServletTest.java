package com.netflix.priam.metrics;

import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.Response;

import mockit.Mocked;
import mockit.NonStrict;
import mockit.NonStrictExpectations;

import org.junit.Assert;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.collect.Lists;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.resources.HecubaServlet;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.CassandraMonitor;

public class HecubaServletTest {

	private @Mocked @NonStrict PriamServer priamServer;
    private @Mocked @NonStrict IConfiguration config;
    private @Mocked @NonStrict HecubaTask task;
    private @Mocked @NonStrict PriamScheduler scheduler;
	private HecubaServlet servlet;
	
	
	@Test
	public void testGetMetrics() throws Exception {
		
		CassandraMonitor.setIsCassandraStarted();
		final List<MetricDatum> mDatums = Lists.newArrayList();
		
		Dimension dimension = new Dimension().withName("testapp").withValue("myinstance");
		MetricDatum datum1 = MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY+":myinstance", Arrays.asList(dimension), 
				StandardUnit.Bytes, new Double(13));
		mDatums.add(datum1);
		mDatums.add(MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_CURRENT_THREAD_COUNT+":myinstance", Arrays.asList(dimension), 
				StandardUnit.Count, new Double(12)));
		
		
		new NonStrictExpectations() {
			MetricsCollector hcm;
            {	
            	servlet = new HecubaServlet(priamServer, config, hcm, scheduler, task);
            	hcm.collectMetrics(); result = mDatums;
            }
            
		};
		
		Response res = servlet.getMetrics();
		Object entity = res.getEntity();
		Assert.assertTrue(entity.toString().contains("hecuba metrics"));
		Assert.assertTrue(entity.toString().contains("testapp"));
		Assert.assertTrue(entity.toString().contains(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY));
		
	}
	
	
	@Test
	public void testEnableDisable() throws Exception {
		
		createExpectations(null, true);
		Assert.assertTrue(servlet.enableMetrics().getEntity().toString().contains("enabled"));
		createExpectations(null, false);
		Assert.assertTrue(servlet.disableMetrics().getEntity().toString().contains("disabled"));
	}
	
	@Test
	public void testSend() throws Exception {
		new NonStrictExpectations() {
			PriamScheduler scheduler;
			MetricsCollector hcm;
			HecubaTask task;
			{
				servlet = new HecubaServlet(priamServer, config, hcm, scheduler, task);
				task.execute(); times = 1;
			}
		};
		servlet.sendMetrics();
	}

	
	@Test
	public void testStatus() throws Exception {
		
		new NonStrictExpectations() {
			MetricsCollector hcm;
			{
				servlet = new HecubaServlet(priamServer, config, hcm, scheduler, task);
			}
		};
		Response res = servlet.getStatus();
		Assert.assertTrue(res.getEntity().toString().contains("non-successful executions"));
	}

	
	
	
	
	private void createExpectations(final String jobName, final boolean metricsEnabled) throws SchedulerException {
		
		new NonStrictExpectations() {
			
			PriamScheduler scheduler;
			Scheduler qScheduler;
			MetricsCollector hcm;
			HecubaTask task;
			{
				servlet = new HecubaServlet(priamServer, config, hcm, scheduler, task);
				scheduler.getScheduler(); result = qScheduler;
				qScheduler.getJobNames(anyString); result = Arrays.asList(jobName, "someOtherJob").toArray();
				task.isMetricsEnabled(); result = metricsEnabled;
				
			}
		};
	}

}
