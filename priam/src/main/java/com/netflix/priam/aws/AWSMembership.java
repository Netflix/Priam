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

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Filter;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.config.InstanceInfo;
import java.util.*;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes in the ASG - Number
 * of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership {
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final IConfiguration config;
    private final ICredential provider;
    private final InstanceInfo instanceInfo;
    private final ICredential crossAccountProvider;

    @Inject
    public AWSMembership(
            IConfiguration config,
            ICredential provider,
            @Named("awsec2roleassumption") ICredential crossAccountProvider,
            InstanceInfo instanceInfo) {
        this.config = config;
        this.provider = provider;
        this.instanceInfo = instanceInfo;
        this.crossAccountProvider = crossAccountProvider;
    }

    @Override
    public ImmutableSet<String> getRacMembership() {
        AmazonAutoScaling client = null;
        try {
            List<String> asgNames = new ArrayList<>();
            asgNames.add(instanceInfo.getAutoScalingGroup());
            asgNames.addAll(Arrays.asList(config.getSiblingASGNames().split("\\s*,\\s*")));
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq =
                    new DescribeAutoScalingGroupsRequest()
                            .withAutoScalingGroupNames(
                                    asgNames.toArray(new String[asgNames.size()]));
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            ImmutableSet.Builder<String> instanceIds = ImmutableSet.builder();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating")
                            || ins.getLifecycleState().equalsIgnoreCase("shutting-down")
                            || ins.getLifecycleState().equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.getInstanceId());
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
        } finally {
            if (client != null) client.shutdown();
        }
    }

    /** Actual membership AWS source of truth... */
    @Override
    public int getRacMembershipSize() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq =
                    new DescribeAutoScalingGroupsRequest()
                            .withAutoScalingGroupNames(instanceInfo.getAutoScalingGroup());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }
            logger.info("Query on ASG returning {} instances", size);
            return size;
        } finally {
            if (client != null) client.shutdown();
        }
    }

    @Override
    public ImmutableSet<String> getCrossAccountRacMembership() {
        AmazonAutoScaling client = null;
        try {
            List<String> asgNames = new ArrayList<>();
            asgNames.add(instanceInfo.getAutoScalingGroup());
            asgNames.addAll(Arrays.asList(config.getSiblingASGNames().split("\\s*,\\s*")));
            client = getCrossAccountAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq =
                    new DescribeAutoScalingGroupsRequest()
                            .withAutoScalingGroupNames(
                                    asgNames.toArray(new String[asgNames.size()]));
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            ImmutableSet.Builder<String> instanceIds = ImmutableSet.builder();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating")
                            || ins.getLifecycleState().equalsIgnoreCase("shutting-down")
                            || ins.getLifecycleState().equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.getInstanceId());
            }
            if (logger.isInfoEnabled()) {
                logger.info(
                        String.format(
                                "Querying Amazon returned following instance in the cross-account ASG: %s --> %s",
                                instanceInfo.getRac(), StringUtils.join(instanceIds, ",")));
            }
            return instanceIds.build();
        } finally {
            if (client != null) client.shutdown();
        }
    }

    @Override
    public int getRacCount() {
        return config.getRacs().size();
    }

    private boolean isClassic() {
        return instanceInfo.getInstanceEnvironment() == InstanceInfo.InstanceEnvironment.CLASSIC;
    }

    /**
     * Adding peers' IPs as ingress to the running instance SG. The running instance could be in
     * "classic" or "vpc"
     */
    public void addACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<>();
            ipPermissions.add(
                    new IpPermission()
                            .withFromPort(from)
                            .withIpProtocol("tcp")
                            .withIpRanges(listIPs)
                            .withToPort(to));

            if (isClassic()) {
                client.authorizeSecurityGroupIngress(
                        new AuthorizeSecurityGroupIngressRequest(
                                config.getACLGroupName(), ipPermissions));
                if (logger.isInfoEnabled()) {
                    logger.info("Done adding ACL to classic: " + StringUtils.join(listIPs, ","));
                }
            } else {
                AuthorizeSecurityGroupIngressRequest sgIngressRequest =
                        new AuthorizeSecurityGroupIngressRequest();
                sgIngressRequest.withGroupId(getVpcGoupId());
                // fetch SG group id for vpc account of the running instance.
                client.authorizeSecurityGroupIngress(
                        sgIngressRequest.withIpPermissions(
                                ipPermissions)); // Adding peers' IPs as ingress to the running
                // instance SG
                if (logger.isInfoEnabled()) {
                    logger.info("Done adding ACL to vpc: " + StringUtils.join(listIPs, ","));
                }
            }

        } finally {
            if (client != null) client.shutdown();
        }
    }

    /*
     * @return SG group id for a group name, vpc account of the running instance.
     */
    protected String getVpcGoupId() {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            Filter nameFilter =
                    new Filter().withName("group-name").withValues(config.getACLGroupName()); // SG
            Filter vpcFilter = new Filter().withName("vpc-id").withValues(instanceInfo.getVpcId());

            DescribeSecurityGroupsRequest req =
                    new DescribeSecurityGroupsRequest().withFilters(nameFilter, vpcFilter);
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups()) {
                logger.debug(
                        "got group-id:{} for group-name:{},vpc-id:{}",
                        group.getGroupId(),
                        config.getACLGroupName(),
                        instanceInfo.getVpcId());
                return group.getGroupId();
            }
            logger.error(
                    "unable to get group-id for group-name={} vpc-id={}",
                    config.getACLGroupName(),
                    instanceInfo.getVpcId());
            return "";
        } finally {
            if (client != null) client.shutdown();
        }
    }

    /** removes a iplist from the SG */
    public void removeACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<>();
            ipPermissions.add(
                    new IpPermission()
                            .withFromPort(from)
                            .withIpProtocol("tcp")
                            .withIpRanges(listIPs)
                            .withToPort(to));

            if (isClassic()) {
                client.revokeSecurityGroupIngress(
                        new RevokeSecurityGroupIngressRequest(
                                config.getACLGroupName(), ipPermissions));
                if (logger.isInfoEnabled()) {
                    logger.info(
                            "Done removing from ACL within classic env for running instance: "
                                    + StringUtils.join(listIPs, ","));
                }
            } else {
                RevokeSecurityGroupIngressRequest req = new RevokeSecurityGroupIngressRequest();
                // fetch SG group id for vpc account of the running instance.
                req.withGroupId(getVpcGoupId());
                // Adding peers' IPs as ingress to the running instance SG
                client.revokeSecurityGroupIngress(req.withIpPermissions(ipPermissions));
                if (logger.isInfoEnabled()) {
                    logger.info(
                            "Done removing from ACL within vpc env for running instance: "
                                    + StringUtils.join(listIPs, ","));
                }
            }

        } finally {
            if (client != null) client.shutdown();
        }
    }

    /** List SG ACL's */
    public ImmutableSet<String> listACL(int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            ImmutableSet.Builder<String> ipPermissions = ImmutableSet.builder();

            if (isClassic()) {

                DescribeSecurityGroupsRequest req =
                        new DescribeSecurityGroupsRequest()
                                .withGroupNames(
                                        Collections.singletonList(config.getACLGroupName()));
                DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
                for (SecurityGroup group : result.getSecurityGroups())
                    for (IpPermission perm : group.getIpPermissions())
                        if (perm.getFromPort() == from && perm.getToPort() == to)
                            ipPermissions.addAll(perm.getIpRanges());

                logger.debug("Fetch current permissions for classic env of running instance");
            } else {

                Filter nameFilter =
                        new Filter().withName("group-name").withValues(config.getACLGroupName());
                String vpcid = instanceInfo.getVpcId();
                if (vpcid == null || vpcid.isEmpty()) {
                    throw new IllegalStateException(
                            "vpcid is null even though instance is running in vpc.");
                }

                // only fetch SG for the vpc id of the running instance
                Filter vpcFilter = new Filter().withName("vpc-id").withValues(vpcid);
                DescribeSecurityGroupsRequest req =
                        new DescribeSecurityGroupsRequest().withFilters(nameFilter, vpcFilter);
                DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
                for (SecurityGroup group : result.getSecurityGroups())
                    for (IpPermission perm : group.getIpPermissions())
                        if (perm.getFromPort() == from && perm.getToPort() == to)
                            ipPermissions.addAll(perm.getIpRanges());

                logger.debug("Fetch current permissions for vpc env of running instance");
            }

            return ipPermissions.build();
        } finally {
            if (client != null) client.shutdown();
        }
    }

    @Override
    public void expandRacMembership(int count) {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq =
                    new DescribeAutoScalingGroupsRequest()
                            .withAutoScalingGroupNames(instanceInfo.getAutoScalingGroup());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            AutoScalingGroup asg = res.getAutoScalingGroups().get(0);
            UpdateAutoScalingGroupRequest ureq = new UpdateAutoScalingGroupRequest();
            ureq.setAutoScalingGroupName(asg.getAutoScalingGroupName());
            ureq.setMinSize(asg.getMinSize() + 1);
            ureq.setMaxSize(asg.getMinSize() + 1);
            ureq.setDesiredCapacity(asg.getMinSize() + 1);
            client.updateAutoScalingGroup(ureq);
        } finally {
            if (client != null) client.shutdown();
        }
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        return AmazonAutoScalingClientBuilder.standard()
                .withCredentials(provider.getAwsCredentialProvider())
                .withRegion(instanceInfo.getRegion())
                .build();
    }

    protected AmazonAutoScaling getCrossAccountAutoScalingClient() {
        return AmazonAutoScalingClientBuilder.standard()
                .withCredentials(crossAccountProvider.getAwsCredentialProvider())
                .withRegion(instanceInfo.getRegion())
                .build();
    }

    protected AmazonEC2 getEc2Client() {
        return AmazonEC2ClientBuilder.standard()
                .withCredentials(provider.getAwsCredentialProvider())
                .withRegion(instanceInfo.getRegion())
                .build();
    }
}
