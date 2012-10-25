package com.netflix.priam.config;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public class CassandraConfiguration {
    @JsonProperty
    private String partitionerClassName = "org.apache.cassandra.dht.RandomPartitioner";

    @JsonProperty
    private String minimumToken;

    @JsonProperty
    private String maximumToken;

    @JsonProperty
    private String bootstrapClusterName;

    @JsonProperty
    private boolean multiRegionEnabled;

    @JsonProperty
    private String endpointSnitch;

    @JsonProperty
    private String cassHome;

    @JsonProperty
    private String cassStartScript;

    @JsonProperty
    private String cassStopScript;

    @JsonProperty
    private String clusterName;

    @JsonProperty
    private String dataLocation;

    @JsonProperty
    private int sslStoragePort;

    @JsonProperty
    private int storagePort;

    @JsonProperty
    private int thriftPort;

    @JsonProperty
    private int jmxPort;

    @JsonProperty
    private int compactionThroughputMBPerSec;

    @JsonProperty
    private int inMemoryCompactionLimitMB;

    @JsonProperty
    private Map<String, String> directMaxHeapSize;

    @JsonProperty
    private Map<String, String> maxHeapSize;

    @JsonProperty
    private Map<String, String> maxNewGenHeapSize;

    @JsonProperty
    private String heapDumpLocation;

    @JsonProperty
    private int memTableTotalSpaceMB;

    @JsonProperty
    private long hintedHandoffThrottleDelayMS;

    @JsonProperty
    private long maxHintWindowMS;

    @JsonProperty
    private boolean localBootstrapEnable;

    @JsonProperty
    private String cacheLocation;

    @JsonProperty
    private String seedProviderClassName;

    @JsonProperty
    private String keyCacheSizeInMB;

    @JsonProperty
    private String keyCacheKeysToSave;

    @JsonProperty
    private String rowCacheSizeInMB;

    @JsonProperty
    private String rowCacheKeysToSave;

    public String getPartitionerClassName() {
        return partitionerClassName;
    }

    public String getMinimumToken() {
        return minimumToken;
    }

    public String getMaximumToken() {
        return maximumToken;
    }

    public String getBootstrapClusterName() {
        return bootstrapClusterName;
    }

    public boolean isMultiRegionEnabled() {
        return multiRegionEnabled;
    }

    public String getEndpointSnitch() {
        return endpointSnitch;
    }

    public String getCassHome() {
        return cassHome;
    }

    public String getCassStartScript() {
        return cassStartScript;
    }

    public String getCassStopScript() {
        return cassStopScript;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getDataLocation() {
        return dataLocation;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public int getCompactionThroughputMBPerSec() {
        return compactionThroughputMBPerSec;
    }

    public int getInMemoryCompactionLimitMB() {
        return inMemoryCompactionLimitMB;
    }

    public Map<String, String> getDirectMaxHeapSize() {
        return directMaxHeapSize;
    }

    public Map<String, String> getMaxHeapSize() {
        return maxHeapSize;
    }

    public Map<String, String> getMaxNewGenHeapSize() {
        return maxNewGenHeapSize;
    }

    public String getHeapDumpLocation() {
        return heapDumpLocation;
    }

    public int getMemTableTotalSpaceMB() {
        return memTableTotalSpaceMB;
    }

    public long getHintedHandoffThrottleDelayMS() {
        return hintedHandoffThrottleDelayMS;
    }

    public long getMaxHintWindowMS() {
        return maxHintWindowMS;
    }

    public boolean isLocalBootstrapEnable() {
        return localBootstrapEnable;
    }

    public String getCacheLocation() {
        return cacheLocation;
    }

    public String getSeedProviderClassName() {
        return seedProviderClassName;
    }

    public String getKeyCacheSizeInMB() {
        return keyCacheSizeInMB;
    }

    public String getKeyCacheKeysToSave() {
        return keyCacheKeysToSave;
    }

    public String getRowCacheSizeInMB() {
        return rowCacheSizeInMB;
    }

    public String getRowCacheKeysToSave() {
        return rowCacheKeysToSave;
    }

    public void setPartitionerClassName(String partitionerClassName) {
        this.partitionerClassName = partitionerClassName;
    }

    public void setMinimumToken(String minimumToken) {
        this.minimumToken = minimumToken;
    }

    public void setMaximumToken(String maximumToken) {
        this.maximumToken = maximumToken;
    }

    public void setBootstrapClusterName(String bootstrapClusterName) {
        this.bootstrapClusterName = bootstrapClusterName;
    }

    public void setMultiRegionEnabled(boolean multiRegionEnabled) {
        this.multiRegionEnabled = multiRegionEnabled;
    }

    public void setEndpointSnitch(String endpointSnitch) {
        this.endpointSnitch = endpointSnitch;
    }

    public void setCassHome(String cassHome) {
        this.cassHome = cassHome;
    }

    public void setCassStartScript(String cassStartScript) {
        this.cassStartScript = cassStartScript;
    }

    public void setCassStopScript(String cassStopScript) {
        this.cassStopScript = cassStopScript;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setDataLocation(String dataLocation) {
        this.dataLocation = dataLocation;
    }

    public void setSslStoragePort(int sslStoragePort) {
        this.sslStoragePort = sslStoragePort;
    }

    public void setStoragePort(int storagePort) {
        this.storagePort = storagePort;
    }

    public void setThriftPort(int thriftPort) {
        this.thriftPort = thriftPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public void setCompactionThroughputMBPerSec(int compactionThroughputMBPerSec) {
        this.compactionThroughputMBPerSec = compactionThroughputMBPerSec;
    }

    public void setInMemoryCompactionLimitMB(int inMemoryCompactionLimitMB) {
        this.inMemoryCompactionLimitMB = inMemoryCompactionLimitMB;
    }

    public void setDirectMaxHeapSize(Map<String, String> directMaxHeapSize) {
        this.directMaxHeapSize = directMaxHeapSize;
    }

    public void setMaxHeapSize(Map<String, String> maxHeapSize) {
        this.maxHeapSize = maxHeapSize;
    }

    public void setMaxNewGenHeapSize(Map<String, String> maxNewGenHeapSize) {
        this.maxNewGenHeapSize = maxNewGenHeapSize;
    }

    public void setHeapDumpLocation(String heapDumpLocation) {
        this.heapDumpLocation = heapDumpLocation;
    }

    public void setMemTableTotalSpaceMB(int memTableTotalSpaceMB) {
        this.memTableTotalSpaceMB = memTableTotalSpaceMB;
    }

    public void setHintedHandoffThrottleDelayMS(long hintedHandoffThrottleDelayMS) {
        this.hintedHandoffThrottleDelayMS = hintedHandoffThrottleDelayMS;
    }

    public void setMaxHintWindowMS(long maxHintWindowMS) {
        this.maxHintWindowMS = maxHintWindowMS;
    }

    public void setLocalBootstrapEnable(boolean localBootstrapEnable) {
        this.localBootstrapEnable = localBootstrapEnable;
    }

    public void setCacheLocation(String cacheLocation) {
        this.cacheLocation = cacheLocation;
    }

    public void setSeedProviderClassName(String seedProviderClassName) {
        this.seedProviderClassName = seedProviderClassName;
    }

    public void setKeyCacheSizeInMB(String keyCacheSizeInMB) {
        this.keyCacheSizeInMB = keyCacheSizeInMB;
    }

    public void setKeyCacheKeysToSave(String keyCacheKeysToSave) {
        this.keyCacheKeysToSave = keyCacheKeysToSave;
    }

    public void setRowCacheSizeInMB(String rowCacheSizeInMB) {
        this.rowCacheSizeInMB = rowCacheSizeInMB;
    }

    public void setRowCacheKeysToSave(String rowCacheKeysToSave) {
        this.rowCacheKeysToSave = rowCacheKeysToSave;
    }

}
