package com.netflix.priam.aws;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.ICredential;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.IMembership;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership {
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final AmazonConfiguration amazonConfiguration;
    private final ICredential provider;

    @Inject
    public AWSMembership(AmazonConfiguration amazonConfiguration, ICredential provider) {
        this.amazonConfiguration = amazonConfiguration;
        this.provider = provider;
    }

    @Override
    public List<String> getAutoScaleGroupMembership() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(amazonConfiguration.getAutoScaleGroupName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances()) {
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated"))) {
                        instanceIds.add(ins.getInstanceId());
                    }
                }
            }
            logger.info(String.format("Querying Amazon returned the following instances in the ASG: %s --> %s", amazonConfiguration.getAutoScaleGroupName(), StringUtils.join(instanceIds, ",")));
            return instanceIds;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getAvailabilityZoneMembershipSize() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(amazonConfiguration.getAutoScaleGroupName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }
            logger.info(String.format("Max size of ASG is %d instances", size));
            return size;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public int getUsableAvailabilityZones() {
        return amazonConfiguration.getUsableAvailabilityZones().size();
    }

    /**
     * Adds a iplist to the SG.
     */
    public void addACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(amazonConfiguration.getSecurityGroupName(), ipPermissions));
            logger.info("Done adding ACL to: " + StringUtils.join(listIPs, ","));
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * removes a iplist from the SG
     */
    public void removeACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(amazonConfiguration.getSecurityGroupName(), ipPermissions));
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL(int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<String>();
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(amazonConfiguration.getSecurityGroupName()));
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups()) {
                for (IpPermission perm : group.getIpPermissions()) {
                    if (perm.getFromPort() == from && perm.getToPort() == to) {
                        ipPermissions.addAll(perm.getIpRanges());
                    }
                }
            }

            return ipPermissions;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public void expandAvailabilityZoneMembership(int count) {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(amazonConfiguration.getAutoScaleGroupName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            AutoScalingGroup asg = res.getAutoScalingGroups().get(0);
            UpdateAutoScalingGroupRequest ureq = new UpdateAutoScalingGroupRequest();
            ureq.setAutoScalingGroupName(asg.getAutoScalingGroupName());
            ureq.setMinSize(asg.getMinSize() + 1);
            ureq.setMaxSize(asg.getMinSize() + 1);
            ureq.setDesiredCapacity(asg.getMinSize() + 1);
            client.updateAutoScalingGroup(ureq);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider.getCredentials());
        client.setEndpoint("autoscaling." + amazonConfiguration.getRegionName() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(provider.getCredentials());
        client.setEndpoint("ec2." + amazonConfiguration.getRegionName() + ".amazonaws.com");
        return client;
    }
}
