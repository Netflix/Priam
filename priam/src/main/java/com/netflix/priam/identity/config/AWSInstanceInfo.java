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

import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.utils.RetryableCallable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.imds.Ec2MetadataClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.AvailabilityZone;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class AWSInstanceInfo implements InstanceInfo {
    private static final Logger logger = LoggerFactory.getLogger(AWSInstanceInfo.class);

    private static final String LATEST_METADATA = "/latest/meta-data/";
    static final String PUBLIC_HOSTNAME_URL = LATEST_METADATA + "public-hostname";
    static final String LOCAL_HOSTNAME_URL = LATEST_METADATA + "local-hostname";
    static final String PUBLIC_HOSTIP_URL = LATEST_METADATA + "public-ipv4";
    static final String LOCAL_HOSTIP_URL = LATEST_METADATA + "local-ipv4";
    private static final String AVAILABILITY_ZONE = LATEST_METADATA + "placement/availability-zone";
    private static final String INSTANCE_ID = LATEST_METADATA + "instance-id";
    private static final String INSTANCE_TYPE = LATEST_METADATA + "instance-type";
    private static final String REGION = LATEST_METADATA + "placement/region";
    private static final String MAC = LATEST_METADATA + "mac";

    private String privateIp;
    private String hostIP;
    private String rac;
    private String hostName;
    private String instanceId;
    private String instanceType;
    private String mac;
    private String region;
    private IS3Credential credential;
    private String vpcId;
    private InstanceEnvironment instanceEnvironment;

    @Inject
    public AWSInstanceInfo(IS3Credential credential) {
        this.credential = credential;
    }

    @Override
    public String getPrivateIP() {
        if (privateIp == null) {
            privateIp = tryGetDataFromUrl(LOCAL_HOSTIP_URL);
        }
        return privateIp;
    }

    @Override
    public String getRac() {
        if (rac == null) {
            rac = tryGetDataFromUrl(AVAILABILITY_ZONE);
        }
        return rac;
    }

    @Override
    public List<String> getDefaultRacks() {
        try (Ec2Client client = getEc2Client()) {
            return client.describeAvailabilityZones()
                    .availabilityZones()
                    .stream()
                    .filter(zone -> zone.stateAsString().equals("available"))
                    .map(AvailabilityZone::zoneName)
                    .limit(3)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public String getInstanceId() {
        if (instanceId == null) {
            instanceId = tryGetDataFromUrl(INSTANCE_ID);
        }
        return instanceId;
    }

    @Override
    public String getInstanceType() {
        if (instanceType == null) {
            instanceType = tryGetDataFromUrl(INSTANCE_TYPE);
        }
        return instanceType;
    }

    private String getMac() {
        if (mac == null) {
            mac = tryGetDataFromUrl(MAC);
        }
        return mac;
    }

    @Override
    public String getRegion() {
        if (region == null) {
            region = tryGetDataFromUrl(REGION);
        }
        return region;
    }

    @Override
    public String getVpcId() {
        if (vpcId == null) {
            String mac = getMac();
            if (!StringUtils.isEmpty(mac)) {
                vpcId = tryGetDataFromUrl(LATEST_METADATA + "network/interfaces/macs/" + mac + "/vpc-id");
            }
        }
        return vpcId;
    }

    @Override
    public String getAutoScalingGroup() {
        try (Ec2Client client = getEc2Client()) {
            return new RetryableCallable<String>(15, 30000) {
                public String retriableCall() throws IllegalStateException {
                    DescribeInstancesRequest desc =
                            DescribeInstancesRequest.builder().instanceIds(getInstanceId()).build();
                    Optional<Tag> matchingTag =
                            client.describeInstances(desc)
                                    .reservations()
                                    .stream()
                                    .flatMap(res -> res.instances().stream())
                                    .flatMap(instance -> instance.tags().stream())
                                    .filter(tag -> tag.key().equals("aws:autoscaling:groupName"))
                                    .findFirst();
                    if (matchingTag.isPresent()) {
                        return matchingTag.get().value();
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
            instanceEnvironment = (getVpcId() == null) ? InstanceEnvironment.CLASSIC : InstanceEnvironment.VPC;
        }
        return instanceEnvironment;
    }

    @Override
    public String getHostname() {
        if (hostName == null) {
            String publicHostName = tryGetDataFromUrl(PUBLIC_HOSTNAME_URL);
            hostName = publicHostName == null ? tryGetDataFromUrl(LOCAL_HOSTNAME_URL) : publicHostName;
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

    private Ec2Client getEc2Client() {
        return Ec2Client.builder()
                .region(Region.of(getRegion()))
                .credentialsProvider(credential.getAwsCredentialProvider())
                .build();
    }

    String tryGetDataFromUrl(String url) {
        try (Ec2MetadataClient client = Ec2MetadataClient.create()) {
            // trim()? -> V2 client sometimes appends newlines
            return client.get(url).asString().trim();
        } catch (Exception e) {
            return null;
        }
    }
}
