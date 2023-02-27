/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.identity.config;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.utils.RetryableCallable;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AWSInstanceInfo implements InstanceInfo {
    private static final Logger logger = LoggerFactory.getLogger(AWSInstanceInfo.class);
    static final String PUBLIC_HOSTNAME_URL = "/latest/meta-data/public-hostname";
    static final String LOCAL_HOSTNAME_URL = "/latest/meta-data/local-hostname";
    static final String PUBLIC_HOSTIP_URL = "/latest/meta-data/public-ipv4";
    static final String LOCAL_HOSTIP_URL = "/latest/meta-data/local-ipv4";
    private JSONObject identityDocument = null;
    private String privateIp;
    private String hostIP;
    private String rac;
    private String hostName;
    private String instanceId;
    private String instanceType;
    private String mac;
    private String region;
    private ICredential credential;
    private String vpcId;
    private InstanceEnvironment instanceEnvironment;

    @Inject
    public AWSInstanceInfo(ICredential credential) {
        this.credential = credential;
    }

    @Override
    public String getPrivateIP() {
        if (privateIp == null) {
            privateIp = EC2MetadataUtils.getPrivateIpAddress();
        }
        return privateIp;
    }

    @Override
    public String getRac() {
        if (rac == null) {
            rac = EC2MetadataUtils.getAvailabilityZone();
        }
        return rac;
    }

    @Override
    public List<String> getDefaultRacks() {
        // Get the fist 3 available zones in the region
        AmazonEC2 client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(credential.getAwsCredentialProvider())
                        .withRegion(getRegion())
                        .build();
        DescribeAvailabilityZonesResult res = client.describeAvailabilityZones();
        List<String> zone = Lists.newArrayList();
        for (AvailabilityZone reg : res.getAvailabilityZones()) {
            if (reg.getState().equals("available")) zone.add(reg.getZoneName());
            if (zone.size() == 3) break;
        }
        return ImmutableList.copyOf(zone);
    }

    @Override
    public String getInstanceId() {
        if (instanceId == null) {
            instanceId = EC2MetadataUtils.getInstanceId();
        }
        return instanceId;
    }

    @Override
    public String getInstanceType() {
        if (instanceType == null) {
            instanceType = EC2MetadataUtils.getInstanceType();
        }
        return instanceType;
    }

    private String getMac() {
        if (mac == null) {
            mac = EC2MetadataUtils.getNetworkInterfaces().get(0).getMacAddress();
        }
        return mac;
    }

    @Override
    public String getRegion() {
        if (region == null) {
            region = EC2MetadataUtils.getEC2InstanceRegion();
        }
        return region;
    }

    @Override
    public String getVpcId() {
        String nacId = getMac();
        if (StringUtils.isEmpty(nacId)) return null;

        if (vpcId == null)
            try {
                vpcId = EC2MetadataUtils.getNetworkInterfaces().get(0).getVpcId();
            } catch (Exception e) {
                logger.info(
                        "Vpc id does not exist for running instance, not fatal as running instance maybe not be in vpc.  Msg: {}",
                        e.getLocalizedMessage());
            }

        return vpcId;
    }

    @Override
    public String getAutoScalingGroup() {
        final AmazonEC2 client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(credential.getAwsCredentialProvider())
                        .withRegion(getRegion())
                        .build();
        try {
            return new RetryableCallable<String>(15, 30000) {
                public String retriableCall() throws IllegalStateException {
                    DescribeInstancesRequest desc =
                            new DescribeInstancesRequest().withInstanceIds(getInstanceId());
                    DescribeInstancesResult res = client.describeInstances(desc);

                    for (Reservation resr : res.getReservations()) {
                        for (Instance ins : resr.getInstances()) {
                            for (com.amazonaws.services.ec2.model.Tag tag : ins.getTags()) {
                                if (tag.getKey().equals("aws:autoscaling:groupName"))
                                    return tag.getValue();
                            }
                        }
                    }

                    throw new IllegalStateException("Couldn't determine ASG name");
                }
            }.call();
        } catch (Exception e) {
            logger.error("Failed to determine ASG name.", e);
            return null;
        }
    }

    @Override
    public InstanceEnvironment getInstanceEnvironment() {
        if (instanceEnvironment == null) {
            instanceEnvironment =
                    (getVpcId() == null) ? InstanceEnvironment.CLASSIC : InstanceEnvironment.VPC;
        }
        return instanceEnvironment;
    }

    @Override
    public String getHostname() {
        if (hostName == null) {
            String publicHostName = tryGetDataFromUrl(PUBLIC_HOSTNAME_URL);
            hostName =
                    publicHostName == null ? tryGetDataFromUrl(LOCAL_HOSTNAME_URL) : publicHostName;
        }
        return hostName;
    }

    @Override
    public String getHostIP() {
        if (hostIP == null) {
            String publicHostIP = tryGetDataFromUrl(PUBLIC_HOSTIP_URL);
            hostIP = publicHostIP == null ? tryGetDataFromUrl(LOCAL_HOSTIP_URL) : publicHostIP;
        }
        return hostIP;
    }

    String tryGetDataFromUrl(String url) {
        try {
            return EC2MetadataUtils.getData(url);
        } catch (Exception e) {
            return null;
        }
    }
}
