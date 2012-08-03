package com.netflix.priam;

import com.netflix.priam.config.AmazonConfiguration;

import java.util.Arrays;

public class TestAmazonConfiguration extends AmazonConfiguration {

    public TestAmazonConfiguration(String clusterName, String region, String zone, String instanceId) {
        setRegionName(region);
        setAvailabilityZone(zone);
        setInstanceID(instanceId);
        setUsableAvailabilityZones(Arrays.asList("az1", "az2", "az3"));
        setPublicHostName(instanceId);
        setSecurityGroupName(clusterName);
        setPublicIP(null);
    }
}
