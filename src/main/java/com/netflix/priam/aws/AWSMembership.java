package com.netflix.priam.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
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

    private final IConfiguration config;
    private final BasicAWSCredentials cred;

    @Inject
    public AWSMembership(IConfiguration config, ICredential provider) {
        this.config = config;
        cred = new BasicAWSCredentials(provider.getAccessKeyId(), provider.getSecretAccessKey());
    }

    @Override
    public List<String> getRacMembership() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(config.getASGName()));

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase(InstanceStateName.ShuttingDown.toString())
                            || ins.getLifecycleState().equalsIgnoreCase(InstanceStateName.Terminated.toString())))
                        instanceIds.add(ins.getInstanceId());
            }
            logger.info(String.format("Querying Amazon returned following instance in the ASG: %s --> %s", config.getRac(), StringUtils.join(instanceIds, ",")));
            return instanceIds;
        } finally {
            if (client != null) client.shutdown();
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getRacMembershipSize() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(config.getASGName()));
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }
            logger.info(String.format("Query on ASG returning %d instances", size));
            return size;
        } finally {
            if (client != null) client.shutdown();
        }
    }

    @Override
    public int getRacCount() {
        return config.getRacs().size();
    }

    /**
     * Adds a iplist to the SG.
     */

    public void addACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();

            ipPermissions.add(new IpPermission()
                    .withFromPort(from)
                    .withIpProtocol("tcp")
                    .withIpRanges(listIPs)
                    .withToPort(to));

            client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(config.getAppName(), ipPermissions));
            logger.info("Done adding ACL to: " + StringUtils.join(listIPs, ","));
        } finally {
            if (client != null) client.shutdown();
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

            ipPermissions.add(new IpPermission()
                    .withFromPort(from)
                    .withIpProtocol("tcp")
                    .withIpRanges(listIPs)
                    .withToPort(to));

            client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(config.getAppName(), ipPermissions));
        } finally {
            if (client != null) client.shutdown();
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL() {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<String>();

            DescribeSecurityGroupsResult result = client.describeSecurityGroups(
                    new DescribeSecurityGroupsRequest()
                            .withGroupNames(Arrays.asList(config.getAppName())));

            for (SecurityGroup group : result.getSecurityGroups())
                for (IpPermission perm : group.getIpPermissions())
                    ipPermissions.addAll(perm.getIpRanges());

            return ipPermissions;
        } finally {
            if (client != null) client.shutdown();
        }
    }

    @Override
    public void expandRacMembership(int count) {
        AmazonAutoScaling client = null;

        try {
            client = getAutoScalingClient();
            AutoScalingGroup asg = client.describeAutoScalingGroups(
                    new DescribeAutoScalingGroupsRequest()
                            .withAutoScalingGroupNames(config.getASGName()))
                    .getAutoScalingGroups().get(0);

            client.updateAutoScalingGroup(new UpdateAutoScalingGroupRequest()
                    .withAutoScalingGroupName(asg.getAutoScalingGroupName())
                    .withMinSize(asg.getMinSize() + 1)
                    .withMaxSize(asg.getMaxSize() + 1)
                    .withDesiredCapacity(asg.getDesiredCapacity() + 1));
        } finally {
            if (client != null) client.shutdown();
        }
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        AmazonAutoScaling client = new AmazonAutoScalingClient(cred);
        client.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(cred);
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}
