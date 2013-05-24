package com.netflix.priam.metrics;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXConnectionException;
import com.netflix.priam.utils.JMXNodeTool;
/**
 * Collector to gather cassandra metrics by using {@link JMXNodeTool} calls that may be useful for monitoring 
 * and analyzing bottlenecks. These comprise only metrics that are not available from AWS by default 
 * (e.g. system space available, etc.)
 * <br/><br/>
 * 
 * Cassandra cluster metrics: amount of nodes alive, keys per CF (estimated), used heap, pending tasks
 * <br/><br/>
 * 
 * Cassandra instance metrics: file load, used system diskspace, current thread count
 * <br/><br/>
 * 
 * OS metrics: used disk space on data partition (in %), used memory (in %), system load
 */

@Singleton
public class MetricsCollector {
	
	private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
	
	private OperatingSystemMXBean osMxBean;
	private ThreadMXBean threadMxBean;
	public JMXNodeTool nodetool;
	private IConfiguration config;
	private Set<String> observedColumnFamilies = Collections.emptySet();
    
	
	public static final double KB = 1024d;
	public static final double MB = 1024*1024d;
	public static final double GB = 1024*1024*1024d;
	public static final double TB = 1024*1024*1024*1024d;

	// cluster metrics
	public static final String CLUSTER = "cluster";
	public static final String CLUSTER_LIVE_NODES = CLUSTER+"_live_nodes";
	public static final String CLUSTER_UNREACHABLE_NODES = CLUSTER+"_unreachable_nodes";
	public static final String CLUSTER_ESTIMATED_KEYS = CLUSTER+"_estimated_keys";
	public static final String CLUSTER_PENDING_TASKS = CLUSTER+"_pending_tasks";
	
	// instance metrics
	private static final String INSTANCE = "instance";
	public static final String INSTANCE_FILE_LOAD = "file_load";
	public static final String INSTANCE_USED_HEAP = "used_heap";
	public static final String INSTANCE_USED_SYSTEM_DISKSPACE = "used_system_diskspace";
	public static final String INSTANCE_SYSTEM_LOAD = "system_load";
	public static final String INSTANCE_FREE_RUNTIME_MEMORY = "free_runtime_memory";
	public static final String INSTANCE_CURRENT_THREAD_COUNT = "current_thread_count";
	
	// cf metrics
	public static final String CF_NAME = "cf_name";
	public static final String CF_PENDING_TASKS = "pending_tasks";
	public static final String CF_ESTIMATED_KEYS = "estimated_keys";
	public static final String CF_WRITE_LATENCY_MICROS = "write_latency_micros";
	public static final String CF_READ_LATENCY_MICROS = "read_latency_micros";
	public static final String CF_USED_DISKSPACE = "used_diskspace";
	
	private static final long BYTES_TO_MEGABYTES = 1024*1024;
	
	@Inject
	public MetricsCollector(IConfiguration config) throws JMXConnectionException {
		this.config = config;
		this.observedColumnFamilies = config.getMetricsForColumnFamilies();
		osMxBean = JMXNodeTool.getRemoteBean(OperatingSystemMXBean.class, 
				ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, config, true);
		threadMxBean = JMXNodeTool.getRemoteBean(ThreadMXBean.class, 
				ManagementFactory.THREAD_MXBEAN_NAME, config, true);
		setNodeTool(JMXNodeTool.connect(config));
		logger.info("Metricscollector instantiated!");
	}
	
	
	// constructor only for testing
	protected MetricsCollector(IConfiguration config2,
			OperatingSystemMXBean osMxBean2, ThreadMXBean threadMxBean2, JMXNodeTool nodetool2) throws JMXConnectionException {
		this.nodetool = nodetool2;
		this.config = config2;
		this.osMxBean = osMxBean2;
		this.threadMxBean = threadMxBean2;
	}

	/**
	 * Wraps statistical info into a DTO
	 * @param entry jmx information from cassandra or the vm
	 * @return a metric DTO
	 */
	private CfMetric createHecubaCfMetric(Entry<String, ColumnFamilyStoreMBean> entry) {
		
		String keyspace = entry.getKey();
		String cfName = entry.getValue().getColumnFamilyName(); 
		long writeCount = entry.getValue().getWriteCount();
		if (writeCount == 0) {
			logger.info("Amount of writes for for CF [" + cfName + "] is zero, inserting 0 instaed");
		}
		long readCount = entry.getValue().getReadCount();
		if (readCount == 0) {
			logger.info("Amount of reads for CF [" + cfName + "] is zero, inserting 0 instaed");
		}
			
		int pendingTasks = entry.getValue().getPendingTasks();
		long estimatedKeys = entry.getValue().estimateKeys();
		long avgWriteLatencyMicroSecs = writeCount == 0 ? 0 : entry.getValue().getTotalWriteLatencyMicros()/writeCount;
		long avgReadLatencyMicroSecs = readCount == 0 ? 0 : entry.getValue().getTotalReadLatencyMicros()/readCount;
		long du = entry.getValue().getTotalDiskSpaceUsed();
		
		CfMetric metric = new CfMetric(keyspace, cfName, pendingTasks, estimatedKeys, 
										avgWriteLatencyMicroSecs, avgReadLatencyMicroSecs, du);
		return metric;
	}
	
	/**
	 * Iterates over column family mbeans and extracts info if column family should be observed
	 * 
	 * @param observedColumnFamilies (defined in priam's properties)
	 * @return a set of column family metrics
	 */
	public Set<CfMetric> getCfStats(Set<String> observedColumnFamilies) {
		
		Iterator<Entry<String, ColumnFamilyStoreMBean>> it = nodetool.getColumnFamilyStoreMBeanProxies();
        Set<CfMetric> cfMetrics = Sets.newHashSet();
        if (observedColumnFamilies == null || observedColumnFamilies.isEmpty()) {
        	logger.info("No column family names given ... returning empty metric set.");
        	return cfMetrics;
        }
        logger.debug("Fetching metadata for ["+observedColumnFamilies+"]");
        while (it.hasNext())
        {	
        	Entry<String, ColumnFamilyStoreMBean> entry = it.next();
        	String keyspace = entry.getKey();
        	String cfName = entry.getValue().getColumnFamilyName();
        	logger.debug("Fetching data for ["+keyspace+"||"+cfName+"]");
        	if (observedColumnFamilies.contains(cfName)) {
        		cfMetrics.add(createHecubaCfMetric(entry));
        	} else {
        		logger.debug("Skipping column family ["+cfName+"]");
        		continue;
        	}
        	
        }
        logger.debug("Column family stats: " + cfMetrics.toString());
        return cfMetrics;
	}

	
	/**
	 * Collects cassandra colum family metrics
	 * 
	 * @return a list of {@link MetricDatum} to be fired into cloudwatch
	 */
	private List<MetricDatum> collectCassandraCfMetrics() {
		
		Set<CfMetric> cfMetrics = getCfStats(observedColumnFamilies);
		List<MetricDatum> mDatums = Lists.newArrayList();
		for (CfMetric cfMetric : cfMetrics) {
			Dimension dimension = new Dimension().withName(config.getAppName()).withValue(cfMetric.getColumnFamilyName());
			mDatums.add(createMetricDatum(CF_ESTIMATED_KEYS, Arrays.asList(dimension), 
					StandardUnit.Count, new Double(cfMetric.getEstimatedKeys())));
			mDatums.add(createMetricDatum(CF_PENDING_TASKS, Arrays.asList(dimension), 
					StandardUnit.Count, new Double(cfMetric.getPendingTasks())));
			mDatums.add(createMetricDatum(CF_USED_DISKSPACE, Arrays.asList(dimension), 
					StandardUnit.Bytes, new Double(cfMetric.getTotalDiskSpaceUsed())));
			mDatums.add(createMetricDatum(CF_READ_LATENCY_MICROS, Arrays.asList(dimension), 
					StandardUnit.Microseconds, new Double(cfMetric.getAvgReadLatencyMicros())));
			mDatums.add(createMetricDatum(CF_WRITE_LATENCY_MICROS, Arrays.asList(dimension), 
					StandardUnit.Microseconds, new Double(cfMetric.getAvgWriteLatencyMicros())));
			
		}
		return mDatums;
	}
	
	/**
	 * Collects cassandra cluster metrics
	 * 
	 * @return a list of {@link MetricDatum} to be fired into AWS cloudwatch
	 */
	private List<MetricDatum> collectCassandraClusterMetrics() {
		
		List<MetricDatum> mDatums = Lists.newArrayList();
		Dimension dimension = new Dimension().withName(config.getAppName()).withValue(CLUSTER);
		mDatums.add(createMetricDatum(CLUSTER_LIVE_NODES, Arrays.asList(dimension), 
				StandardUnit.Count, new Double(nodetool.getLiveNodes().size())));
		mDatums.add(createMetricDatum(CLUSTER_UNREACHABLE_NODES, Arrays.asList(dimension), 
				StandardUnit.Count, new Double(nodetool.getUnreachableNodes().size())));
		// gets the space used for files of each node		
		Map<String, String> loadMap = nodetool.getLoadMap();
		for (Entry<String, String> entry : loadMap.entrySet()) {
			// e.g. 10.0.0.1 = 116,16 KB
			String node = entry.getKey();
			String amount = entry.getValue().split(" ")[0].replace(",", ".");
			String unit = entry.getValue().split(" ")[1];
			mDatums.add(createMetricDatum(INSTANCE_FILE_LOAD+":"+node, Arrays.asList(dimension), 
					computeUnit(unit), new Double(amount)));
		}
		return mDatums;
	}
	
	/**
	 * Collects instance metrics
	 * 
	 * @return a list of {@link MetricDatum} to be fired into cloudwatch
	 */
	private List<MetricDatum> collectCassandraInstanceMetrics() {
		List<MetricDatum> mDatums = Lists.newArrayList();
		Dimension dimension = new Dimension().withName(config.getAppName()).withValue(INSTANCE);
		mDatums.add(createMetricDatum(INSTANCE_USED_HEAP+":"+config.getInstanceName(), Arrays.asList(dimension), 
				StandardUnit.Bytes, new Double(nodetool.getHeapMemoryUsage().getUsed())));
		
		String dataFileLocation = config.getDataFileLocation();
		if (dataFileLocation != null) {
			File file = new File(dataFileLocation);
			Double usedSpacePercent = 1D - (new Double(file.getUsableSpace())/new Double(file.getTotalSpace()));
			mDatums.add(createMetricDatum(INSTANCE_USED_SYSTEM_DISKSPACE+":"+config.getInstanceName(), Arrays.asList(dimension), 
					StandardUnit.Percent, usedSpacePercent*100));
		}
		mDatums.add(createMetricDatum(INSTANCE_SYSTEM_LOAD+":"+config.getInstanceName(), Arrays.asList(dimension), 
				StandardUnit.None, new Double(osMxBean.getSystemLoadAverage())));
		mDatums.add(createMetricDatum(INSTANCE_FREE_RUNTIME_MEMORY+":"+config.getInstanceName(), Arrays.asList(dimension), 
				StandardUnit.Megabytes, new Double(Runtime.getRuntime().freeMemory()/BYTES_TO_MEGABYTES)));
		mDatums.add(createMetricDatum(INSTANCE_CURRENT_THREAD_COUNT+":"+config.getInstanceName(), Arrays.asList(dimension), 
				StandardUnit.Count, new Double(threadMxBean.getThreadCount())));
		
		return mDatums;
	}
	
	
	/**
	 * Collects cassandra related metrics
	 * 
	 * @return a list of {@link MetricDatum}
	 */
	public @Nonnull List<MetricDatum> collectMetrics() {
		
		long start = System.currentTimeMillis();
		List<MetricDatum> mDatums = Lists.newArrayList();
		mDatums.addAll(collectCassandraInstanceMetrics());
		mDatums.addAll(collectCassandraCfMetrics());
		mDatums.addAll(collectCassandraClusterMetrics());
		long end = System.currentTimeMillis();
		logger.info("Collecting Hecuba data took " + (end-start) + " ms.");
		return mDatums;
	}
	

	public static MetricDatum createMetricDatum(String metricName, Collection<Dimension> dimensions, StandardUnit unit, Double value) {
		return createMetricDatum(metricName, dimensions, unit, value, null);
	}
	
	public static MetricDatum createMetricDatum(String metricName, Collection<Dimension> dimensions, StandardUnit unit, StatisticSet statisticSet) {
		return createMetricDatum(metricName, dimensions, unit, null, statisticSet);
	}
	
	/**
	 * Private helper method for creating metric datums with a statisticSet
	 * 
	 * @return a {@link MetricDatum} to be inserted in AWS cloudwatch
	 */
	private static MetricDatum createMetricDatum(String metricName, @Nullable Collection<Dimension> dimensions, 
			StandardUnit unit, Double value, @Nullable StatisticSet statisticSet) {
		
		MetricDatum datum = new MetricDatum().withMetricName(metricName).withDimensions(dimensions)
					.withUnit(unit).withValue(value).withStatisticValues(statisticSet);
		return datum;
	}

	

	// Helper methods
	public static Double createBytesAmountFromStringifiedValue(String unit, String stringifiedValue) {
		stringifiedValue = stringifiedValue.replace(" "+unit, "").trim();
		return new Double(stringifiedValue);
	}
	
	
	public static Double unstringifyFileSize(String stringifiedValue) {
        
		if (stringifiedValue.contains("TB")) {
        	stringifiedValue = stringifiedValue.replace(" TB", "").trim();
        	return new Double(stringifiedValue)*TB;
        } else if (stringifiedValue.contains("GB")) {
        	stringifiedValue = stringifiedValue.replace(" GB", "").trim();
        	return new Double(stringifiedValue)*GB;
        } else if (stringifiedValue.contains("MB")) {
        	stringifiedValue = stringifiedValue.replace(" MB", "").trim();
        	return new Double(stringifiedValue)*MB;
        } else if (stringifiedValue.contains("KB")) {
        	stringifiedValue = stringifiedValue.replace(" KB", "").trim();
        	return new Double(stringifiedValue)*KB;
        } else {
        	stringifiedValue = stringifiedValue.replace(" bytes", "").trim();
        	return new Double(stringifiedValue);
        }
    }
	
	/**
	 * Helper for wrapping textual representation of units into AWS {@link StandardUnit}
	 
	 * @param unit as a string
	 * @return calculated {@link StandardUnit}
	 */
	public static StandardUnit computeUnit(String unit) {
		
		if (unit.contains("TB")) {
        	return StandardUnit.Terabytes;
        } else if (unit.contains("GB")) {
        	return StandardUnit.Gigabytes;
        } else if (unit.contains("MB")) {
        	return StandardUnit.Megabytes;
        } else if (unit.contains("KB")) {
        	return StandardUnit.Kilobytes;
        } else if (unit.contains("ytes")) {
        	return StandardUnit.Bytes;
        } else {
        	throw new IllegalArgumentException("Unit ["+unit+"] not supported.");
        }
	}
	
	// setter only for mocking
	protected void setNodeTool(JMXNodeTool nodeTool) {
		this.nodetool = nodeTool;
	}
	protected void setOsMxBean(OperatingSystemMXBean osMxBean) {
		this.osMxBean = osMxBean;
	}
	protected void setThreadMxBean(ThreadMXBean threadMxBean) {
		this.threadMxBean = threadMxBean;
	}

}
