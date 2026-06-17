/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.aws;

import com.google.common.collect.ImmutableSet;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.config.InstanceInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup;
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsResponse;
import software.amazon.awssdk.services.autoscaling.model.Instance;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes in the ASG - Number
 * of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership {
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final IConfiguration config;
    private final ICredential provider;
    private final InstanceInfo instanceInfo;

    @Inject
    public AWSMembership(
            IConfiguration config,
            ICredential provider,
            InstanceInfo instanceInfo) {
        this.config = config;
        this.provider = provider;
        this.instanceInfo = instanceInfo;
    }

    @Override
    public ImmutableSet<String> getRacMembership() {
        try (AutoScalingClient client = getAutoScalingClient()) {
            List<String> asgNames = new ArrayList<>();
            asgNames.add(instanceInfo.getAutoScalingGroup());
            asgNames.addAll(Arrays.asList(config.getSiblingASGNames().split("\\s*,\\s*")));
            DescribeAutoScalingGroupsRequest asgReq =
                    DescribeAutoScalingGroupsRequest.builder()
                            .autoScalingGroupNames(asgNames)
                            .build();
            DescribeAutoScalingGroupsResponse res = client.describeAutoScalingGroups(asgReq);

            ImmutableSet.Builder<String> instanceIds = ImmutableSet.builder();
            for (AutoScalingGroup asg : res.autoScalingGroups()) {
                for (Instance ins : asg.instances())
                    if (!(ins.lifecycleStateAsString().equalsIgnoreCase("Terminating")
                            || ins.lifecycleStateAsString().equalsIgnoreCase("shutting-down")
                            || ins.lifecycleStateAsString().equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.instanceId());
            }
            if (logger.isInfoEnabled()) {
                logger.info(
                        String.format(
                                "Querying Amazon returned following instance in the RAC: %s, ASGs: %s --> %s",
                                instanceInfo.getRac(),
                                StringUtils.join(asgNames, ","),
                                StringUtils.join(instanceIds, ",")));
            }
            return instanceIds.build();
        }
    }

    /** Actual membership AWS source of truth... */
    @Override
    public int getRacMembershipSize() {
        try (AutoScalingClient client = getAutoScalingClient()) {
            DescribeAutoScalingGroupsRequest asgReq =
                    DescribeAutoScalingGroupsRequest.builder()
                            .autoScalingGroupNames(instanceInfo.getAutoScalingGroup())
                            .build();
            DescribeAutoScalingGroupsResponse res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.autoScalingGroups()) {
                size += asg.maxSize();
            }
            logger.info("Query on ASG returning {} instances", size);
            return size;
        }
    }

    @Override
    public int getRacCount() {
        return config.getRacs().size();
    }

    protected AutoScalingClient getAutoScalingClient() {
        return AutoScalingClient.builder()
                .region(Region.of(instanceInfo.getRegion()))
                .credentialsProvider(provider.getAwsCredentialProvider())
                .build();
    }
}
