package com.netflix.priam;

import com.netflix.priam.config.AmazonConfiguration;

import java.util.Arrays;

public class TestAmazonConfiguration extends AmazonConfiguration {

    public TestAmazonConfiguration(String clusterName, String region, String zone, String instanceId) {
        setRegionName(region);
        setAvailabilityZone(zone);
        setInstanceID(instanceId);
        setInstanceType("m1.xlarge");
        setUsableAvailabilityZones(Arrays.asList("az1", "az2", "az3"));
        setPrivateHostName(instanceId);
        setSecurityGroupName(clusterName);
        setPrivateIP(null);
    }
}
