package com.netflix.priam.config;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.google.common.collect.Lists;
import com.netflix.priam.ICredential;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class AmazonConfiguration {
    @JsonProperty
    private String autoScaleGroupName;

    @JsonProperty
    private String regionName;

    @JsonProperty
    private String securityGroupName;

    @JsonProperty
    private String availabilityZone;

    @JsonProperty
    private String privateHostName;

    @JsonProperty
    private String privateIP;

    @JsonProperty
    private String instanceID;

    @JsonProperty
    private String instanceType;

    @JsonProperty
    private List<String> usableAvailabilityZones;

    @JsonProperty
    private String simpleDbDomain;

    @JsonProperty
    private String simpleDbRegion;   // Defaults to the current region.  Set explicitly for cross-dc rings.

    public String getAutoScaleGroupName() {
        return autoScaleGroupName;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getSecurityGroupName() {
        return securityGroupName;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public String getPrivateHostName() {
        return privateHostName;
    }

    public String getPrivateIP() {
        return privateIP;
    }

    public String getInstanceID() {
        return instanceID;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public List<String> getUsableAvailabilityZones() {
        return usableAvailabilityZones;
    }

    public String getSimpleDbDomain() {
        return simpleDbDomain;
    }

    public String getSimpleDbRegion() {
        return simpleDbRegion;
    }

    public void setAutoScaleGroupName(String autoScaleGroupName) {
        this.autoScaleGroupName = autoScaleGroupName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setSecurityGroupName(String securityGroupName) {
        this.securityGroupName = securityGroupName;
    }

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public void setPrivateHostName(String privateHostName) {
        this.privateHostName = privateHostName;
    }

    public void setPrivateIP(String privateIP) {
        this.privateIP = privateIP;
    }

    public void setInstanceID(String instanceID) {
        this.instanceID = instanceID;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public void setUsableAvailabilityZones(List<String> usableAvailabilityZones) {
        this.usableAvailabilityZones = usableAvailabilityZones;
    }

    public void setSimpleDbDomain(String simpleDbDomain) {
        this.simpleDbDomain = simpleDbDomain;
    }

    public void setSimpleDbRegion(String simpleDbRegion) {
        this.simpleDbRegion = simpleDbRegion;
    }

    public void discoverConfiguration(ICredential credentialProvider) {
        if (StringUtils.isBlank(availabilityZone)) {
            availabilityZone = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
        }
        if (StringUtils.isBlank(regionName)) {
            regionName = StringUtils.isBlank(System.getProperty("EC2_REGION")) ? availabilityZone.substring(0, availabilityZone.length() - 1) : System.getProperty("EC2_REGION");
        }
        if (StringUtils.isBlank(instanceID)) {
            instanceID = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-id");
        }
        if (StringUtils.isBlank(autoScaleGroupName)) {
            autoScaleGroupName = StringUtils.isBlank(System.getProperty("ASG_NAME")) ? populateASGName(credentialProvider, regionName, instanceID) : System.getProperty("ASG_NAME");
        }
        if (StringUtils.isBlank(instanceType)) {
            instanceType = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-type");
        }
        if (StringUtils.isBlank(privateHostName)) {
            privateHostName = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-hostname");
        }
        if (StringUtils.isBlank(privateIP)) {
            privateIP = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-ipv4");
        }
        if (CollectionUtils.isEmpty(usableAvailabilityZones)) {
            usableAvailabilityZones = defineUsableAvailabilityZones(credentialProvider, regionName);
        }
        if (StringUtils.isBlank(securityGroupName)) {
            securityGroupName = populateSecurityGroup(credentialProvider);
        }
    }

    private String populateSecurityGroup(ICredential credentialProvider) {
        AmazonEC2 client = new AmazonEC2Client(credentialProvider.getCredentials());
        client.setEndpoint("ec2." + regionName + ".amazonaws.com");
        try {
            String securityGroupNameOnEachLine = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/security-groups");
            if (StringUtils.isNotBlank(securityGroupNameOnEachLine)) {
                String[] securityGroupNames = StringUtils.split(securityGroupNameOnEachLine, "\n");
                if (securityGroupNames.length >= 1) {
                    return securityGroupNames[0];
                }
            }
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
        return null;
    }


    /**
     * Query amazon to get ASG name. Currently not available as part of instance info api.
     */
    private String populateASGName(ICredential credentialProvider, String region, String instanceId) {
        AmazonEC2 client = new AmazonEC2Client(credentialProvider.getCredentials());
        client.setEndpoint("ec2." + region + ".amazonaws.com");
        try {
            DescribeInstancesRequest desc = new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult res = client.describeInstances(desc);

            for (Reservation resr : res.getReservations()) {
                for (Instance ins : resr.getInstances()) {
                    for (com.amazonaws.services.ec2.model.Tag tag : ins.getTags()) {
                        if (tag.getKey().equals("aws:autoscaling:groupName")) {
                            return tag.getValue();
                        }
                    }
                }
            }
        } finally {
            client.shutdown();
        }
        return null;
    }

    /**
     * Get the fist 3 availability zones in the region
     */
    private List<String> defineUsableAvailabilityZones(ICredential credentialProvider, String region) {
        List<String> zone = Lists.newArrayList();
        AmazonEC2 client = new AmazonEC2Client(credentialProvider.getCredentials());
        client.setEndpoint("ec2." + region + ".amazonaws.com");
        try {
            DescribeAvailabilityZonesResult res = client.describeAvailabilityZones();
            for (AvailabilityZone reg : res.getAvailabilityZones()) {
                if (reg.getState().equals("available")) {
                    zone.add(reg.getZoneName());
                }
                if (zone.size() == 3) {
                    break;
                }
            }
        } finally {
            client.shutdown();
        }
        return zone;
    }
}
