package com.netflix.priam;

import com.netflix.priam.config.CassandraConfiguration;

public class TestCassandraConfiguration extends CassandraConfiguration {

    public TestCassandraConfiguration(String clusterName) {
        setClusterName(clusterName);
        setDataLocation("target/data");
        setCacheLocation("cass/caches");
        setJmxPort(7199);
        setThriftPort(9160);
        setStoragePort(7101);
        setSslStoragePort(7103);
        setEndpointSnitch("org.apache.cassandra.locator.SimpleSnitch");
        setSeedProviderClassName("org.apache.cassandra.locator.SimpleSeedProvider");
        setMultiRegionEnabled(false);
        setLocalBootstrapEnable(false);
        setInMemoryCompactionLimitMB(8);
        setCompactionThroughputMBPerSec(0);
        setBootstrapClusterName("cass_bootstrap");
        setCassStopScript("teststopscript");
        setMaxHintWindowMS(36000);
        setHintedHandoffThrottleDelayMS(0);
        setMemTableTotalSpaceMB(0);
        setKeyCacheSizeInMB(16);
        setKeyCacheKeysToSave(32);
        setRowCacheSizeInMB(4);
        setRowCacheKeysToSave(4);
    }
}
