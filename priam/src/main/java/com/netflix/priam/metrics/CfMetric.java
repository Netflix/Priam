package com.netflix.priam.metrics;


/**
 * Helper class for DTOing CF metadata
 * 
 */
public class CfMetric {
	
	private String keyspace;
	private String columnFamilyName;
	private long pendingTasks;
	private long estimatedKeys;
	private long totalReadLatencyMicros;
	private long totalWriteLatencyMicros;
	private long totalDiskSpaceUsed;
	
	public CfMetric(String keyspace, String columnFamilyName, int pendingTasks, long estimateKeys, 
			long totalWriteLatencyMicros, long totalReadLatencyMicros, long totalDiskSpaceUsed) {
		
		this.keyspace = keyspace;
		this.columnFamilyName = columnFamilyName;
		this.pendingTasks = pendingTasks;
		this.estimatedKeys = estimateKeys;
		this.totalWriteLatencyMicros = totalWriteLatencyMicros;
		this.totalReadLatencyMicros = totalReadLatencyMicros;
		this.totalDiskSpaceUsed = totalDiskSpaceUsed;
		
	}


	public long getTotalDiskSpaceUsed() {
		return totalDiskSpaceUsed;
	}
	public long getTotalReadLatencyMicros() {
		return totalReadLatencyMicros;
	}
	public long getTotalWriteLatencyMicros() {
		return totalWriteLatencyMicros;
	}
	public long getEstimatedKeys() {
		return estimatedKeys;
	}
	public long getPendingTasks() {
		return pendingTasks;
	}
	public String getColumnFamilyName() {
		return columnFamilyName;
	}
	public String getKeyspace() {
		return keyspace;
	}



	@Override
	public int hashCode() {
		return columnFamilyName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CfMetric other = (CfMetric) obj;
		if (columnFamilyName == null) {
			if (other.columnFamilyName != null)
				return false;
		} else if (!columnFamilyName.equals(other.columnFamilyName))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "CfMetric [keyspace=" + keyspace + ", columnFamilyName="
				+ columnFamilyName + ", pendingTasks=" + pendingTasks
				+ ", estimatedKeys=" + estimatedKeys
				+ ", totalReadLatencyMicros=" + totalReadLatencyMicros
				+ ", totalWriteLatencyMicros=" + totalWriteLatencyMicros
				+ ", totalDiskSpaceUsed=" + totalDiskSpaceUsed + "]";
	}

	

}
