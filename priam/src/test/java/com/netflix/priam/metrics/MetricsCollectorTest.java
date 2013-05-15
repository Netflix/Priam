package com.netflix.priam.metrics;

import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;
import mockit.NonStrictExpectations;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.amazonaws.util.json.JSONException;
import com.google.common.collect.Lists;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.JMXConnectionException;
import com.netflix.priam.utils.JMXNodeTool;

public class MetricsCollectorTest {
	
	private MetricsCollector mCollector;
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollectorTest.class);
	
    @Before
    public void setUp() throws JMXConnectionException
    {
    	CassandraMonitor.setIsCassandraStarted();
    }
    
    @Test
	public void testMetricsCollection() throws JSONException, org.codehaus.jettison.json.JSONException, JMXConnectionException {
		
		final List<CfMetric> someHcfMetrics = Lists.newArrayList();
		CfMetric m1 = new CfMetric("keyspace1", "columnFamilyName1", 1, 42, 123456789, 987654321, 100000);
		CfMetric m2 = new CfMetric("keyspace2", "columnFamilyName2", 1, 42, 23456789, 98765432, 200000);
		someHcfMetrics.add(m1);
		someHcfMetrics.add(m2);
		
		
		final List<MetricDatum> mDatums = Lists.newArrayList();
		Dimension dimension = new Dimension().withName("testapp").withValue("myinstance");
		MetricDatum datum1 = MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY+":myinstance", Arrays.asList(dimension), 
				StandardUnit.Bytes, new Double(13));
		mDatums.add(datum1);
		mDatums.add(MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_CURRENT_THREAD_COUNT+":myinstance", Arrays.asList(dimension), 
				StandardUnit.Count, new Double(12)));
		
		final Map<String, String> someMap = new HashMap<String, String>();
		someMap.put("127.0.0.1", "45.8 bytes");
		someMap.put("127.0.0.2", "15.8 TB");
		
		final JSONObject object = new JSONObject();
		object.put("keyspace", "k1");
        object.put("column_family", "ode-to-my-family");
        object.put("estimated_size", "1234567");
        
        final Iterator<Entry<String, ColumnFamilyStoreMBean>> someIter = new ArrayList<Entry<String, ColumnFamilyStoreMBean>>().iterator();
		
		new NonStrictExpectations() {
			
			MemoryUsage memUsage;
			JMXNodeTool nodetool;
			ThreadMXBean threadMxBean;
			OperatingSystemMXBean osMxBean;
			IConfiguration config;

			{
				
				mCollector = new MetricsCollector(config, osMxBean, threadMxBean, nodetool);
				config.getDataFileLocation(); result = null;
				config.getAppName(); result = "testApp";
				config.getInstanceName(); result = "myInstanceName";
				osMxBean.getSystemLoadAverage(); result = 1.23D;
				threadMxBean.getThreadCount(); result = 42;
				
				nodetool.getColumnFamilyStoreMBeanProxies(); result = someIter;
				nodetool.getLiveNodes(); result = Arrays.asList("1");
				nodetool.getUnreachableNodes(); result = Arrays.asList("0");
				nodetool.getLoadMap(); result = someMap;
				nodetool.estimateKeys(); result = object;
				nodetool.getHeapMemoryUsage(); result = memUsage;
				memUsage.getUsed(); result = 123456l;
				nodetool.getLoadMap(); result = someMap;
				
			}
		};
		
		List<MetricDatum> res = mCollector.collectMetrics();
		Map<String, MetricDatum> m = new HashMap<String, MetricDatum>();
		for (MetricDatum metricDatum : res) {
			m.put(metricDatum.getMetricName(), metricDatum);
			logger.info(metricDatum.toString());
		}
		Assert.assertEquals(8, res.size());
		Assert.assertEquals(m.get(MetricsCollector.INSTANCE_USED_HEAP+":myInstanceName").getValue().longValue(), 123456l);
		Assert.assertNotNull(m.get(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY+":myInstanceName").getValue());
		Assert.assertNotNull(m.get(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY+":myInstanceName").getValue());
		Assert.assertNotNull(m.get(MetricsCollector.INSTANCE_CURRENT_THREAD_COUNT+":myInstanceName").getValue());
		Assert.assertNotNull(m.get(MetricsCollector.CLUSTER_LIVE_NODES).getValue());
		Assert.assertNotNull(m.get(MetricsCollector.INSTANCE_FILE_LOAD+":127.0.0.2").getValue());
		
	}
    
	@Test
	public void testCreatingDatums() {
		
		MetricDatum res1; 
		
		res1 = MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY, null, 
				StandardUnit.Bytes, 1.2D);
		
		Assert.assertEquals(1.2D, res1.getValue());
		Assert.assertEquals(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY, res1.getMetricName());
		Assert.assertEquals(Collections.EMPTY_LIST, res1.getDimensions());
		
		
		res1 = MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY, 
				Arrays.asList(new Dimension().withName("dim1").withValue("none")), 
				StandardUnit.Bytes, 1.2D);
		
		Assert.assertEquals(1.2D, res1.getValue());
		Assert.assertEquals(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY, res1.getMetricName());
		Assert.assertEquals("dim1", res1.getDimensions().get(0).getName());
		
		res1 = MetricsCollector.createMetricDatum(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY, 
				Arrays.asList(new Dimension().withName("dim1").withValue("none")), 
				StandardUnit.Bytes, new StatisticSet().withMaximum(10D).withMinimum(2D).withSampleCount(2D).withSum(12D));
		
		Assert.assertEquals(2D, res1.getStatisticValues().getMinimum());
		Assert.assertEquals(MetricsCollector.INSTANCE_FREE_RUNTIME_MEMORY, res1.getMetricName());
		Assert.assertEquals("dim1", res1.getDimensions().get(0).getName());
		Assert.assertEquals(StandardUnit.Bytes.name(), res1.getUnit().toString());
		
	}
	
	@Test
	public void testComputeUnits() {
		
		Assert.assertEquals(MetricsCollector.computeUnit("bytes"), StandardUnit.Bytes);
		Assert.assertEquals(MetricsCollector.computeUnit("KB  "), StandardUnit.Kilobytes);
		Assert.assertEquals(MetricsCollector.computeUnit("   MB"), StandardUnit.Megabytes);
		Assert.assertEquals(MetricsCollector.computeUnit("GB"), StandardUnit.Gigabytes);
		Assert.assertEquals(MetricsCollector.computeUnit("TB"), StandardUnit.Terabytes);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testComputeUnitsEx() {

		MetricsCollector.computeUnit("xB");
	}
	
	
}
