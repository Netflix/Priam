package com.netflix.priam.metrics;

import java.util.List;

import mockit.Mocked;
import mockit.NonStrict;
import mockit.NonStrictExpectations;

import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.util.json.JSONException;
import com.google.common.collect.Lists;
import com.netflix.priam.utils.JMXConnectionException;

public class MetricsSenderTest {
	
	private @NonStrict @Mocked MetricsSender mSender;
	
    
    @Test
	public void testSendMetrics() throws JSONException, org.codehaus.jettison.json.JSONException, JMXConnectionException {
    	
    	final List<MetricDatum> metrics = Lists.newArrayList();
		new NonStrictExpectations() {

			{	
				mSender.sendMetrics(metrics); times = 2;				
			}
		};
		
		mSender.sendMetrics(metrics);
		mSender.sendMetrics(metrics);
	}
    
}
