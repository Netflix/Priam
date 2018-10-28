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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AWSInstanceInfo implements InstanceInfo {
    private static final Logger logger = LoggerFactory.getLogger(AWSInstanceInfo.class);
    private JSONObject identityDocument = null;
    private String privateIp;
    private String publicIp;
    private String rac;
    private String publicHostName;
    private String instanceId;
    private String instanceType;
    private String mac;
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
            privateIp =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/meta-data/local-ipv4");
        }
        return privateIp;
    }

    @Override
    public String getRac() {
        if (rac == null) {
            rac =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/meta-data/placement/availability-zone");
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

    public String getPublicHostname() {
        if (publicHostName == null) {
            publicHostName =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/meta-data/public-hostname");
        }
        return publicHostName;
    }

    public String getPublicIP() {
        if (publicIp == null) {
            publicIp =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/meta-data/public-ipv4");
        }
        return publicIp;
    }

    @Override
    public String getInstanceId() {
        if (instanceId == null) {
            instanceId =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/meta-data/instance-id");
        }
        return instanceId;
    }

    @Override
    public String getInstanceType() {
        if (instanceType == null) {
            instanceType =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/meta-data/instance-type");
        }
        return instanceType;
    }

    private String getMac() {
        if (mac == null) {
            mac =
                    SystemUtils.getDataFromUrl(
                                    "http://169.254.169.254/latest/meta-data/network/interfaces/macs/")
                            .trim();
        }
        return mac;
    }

    @Override
    public String getRegion() {
        try {
            getIdentityDocument();
            return this.identityDocument.getString("region");
        } catch (JSONException e) {
            // If there is any issue in getting region, use AZ as backup.
            return getRac().substring(0, getRac().length() - 1);
        }
    }

    private void getIdentityDocument() throws JSONException {
        if (this.identityDocument == null) {
            String jsonStr =
                    SystemUtils.getDataFromUrl(
                            "http://169.254.169.254/latest/dynamic/instance-identity/document");
            this.identityDocument = new JSONObject(jsonStr);
        }
    }

    @Override
    public String getVpcId() {
        String nacId = getMac();
        if (StringUtils.isEmpty(nacId)) return null;

        if (vpcId == null)
            try {
                vpcId =
                        SystemUtils.getDataFromUrl(
                                        "http://169.254.169.254/latest/meta-data/network/interfaces/macs/"
                                                + nacId
                                                + "vpc-id")
                                .trim();
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
        return (getPublicHostname() == null) ? getPrivateIP() : getPublicHostname();
    }

    @Override
    public String getHostIP() {
        return (getPublicIP() == null) ? getPrivateIP() : getPublicIP();
    }
}
