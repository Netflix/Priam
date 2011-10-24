package com.priam.aws;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.Instance;
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
import com.priam.conf.IConfiguration;
import com.priam.identity.IMembership;

/**
 * Utility class to query amazon ASG for its members
 */
public class AWSMembership implements IMembership
{
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private BasicAWSCredentials cred;
    private IConfiguration config;

    @Inject
    public AWSMembership(IConfiguration config, ICredential provider)
    {
        this.config = config;
        cred = new BasicAWSCredentials(provider.getAccessKeyId(), provider.getSecretAccessKey());
    }

    @Override
    public List<String> getRacMembership()
    {
        AmazonAutoScalingClient asgc = new AmazonAutoScalingClient(cred);
        asgc.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
        DescribeAutoScalingGroupsResult res = asgc.describeAutoScalingGroups(asgReq);

        List<String> instanceIds = Lists.newArrayList();
        for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
            for (Instance ins : asg.getInstances())
                if (!ins.getLifecycleState().equals("Terminating"))
                    instanceIds.add(ins.getInstanceId());
        }
        logger.info(String.format("Querying Amazon returned following instance in the ASG: %s --> %s", config.getRac(), StringUtils.join(instanceIds, ",")));
        return instanceIds;
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getRacMembershipSize()
    {
        AmazonAutoScalingClient asgc = new AmazonAutoScalingClient(cred);
        asgc.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
        DescribeAutoScalingGroupsResult res = asgc.describeAutoScalingGroups(asgReq);
        int size = 0;
        for (AutoScalingGroup asg : res.getAutoScalingGroups())
        {
            size += asg.getMaxSize();
        }
        logger.info(String.format("Query on ASG returning %d instances", size));
        return size;
    }

    @Override
    public int getRacCount()
    {
        return config.getRacs().size();
    }

    /**
     * Adds a iplist to the SG.
     */
    public void addACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = new AmazonEC2Client(cred);
        client.setEndpoint("ec2."+ config.getDC() +".amazonaws.com");
        List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
        IpPermission ip = new IpPermission();
        ip.setFromPort(from);
        ip.setIpProtocol("tcp");
        ip.setIpRanges(listIPs);
        ip.setToPort(to);
        ipPermissions.add(ip);
        AuthorizeSecurityGroupIngressRequest req = new AuthorizeSecurityGroupIngressRequest(config.getAppName(), ipPermissions);
        client.authorizeSecurityGroupIngress(req);
        logger.info("Done adding ACL to: " + StringUtils.join(listIPs, ","));
    }

    /**
     * removes a iplist from the SG
     */
    public void removeACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = new AmazonEC2Client(cred);
        client.setEndpoint("ec2."+ config.getDC() +".amazonaws.com");
        List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
        IpPermission ip = new IpPermission();
        ip.setFromPort(from);
        ip.setIpProtocol("tcp");
        ip.setIpRanges(listIPs);
        ip.setToPort(to);
        ipPermissions.add(ip);
        RevokeSecurityGroupIngressRequest req = new RevokeSecurityGroupIngressRequest(config.getAppName(), ipPermissions);
        client.revokeSecurityGroupIngress(req);
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL()
    {
        AmazonEC2 client = new AmazonEC2Client(cred);
        client.setEndpoint("ec2."+ config.getDC() +".amazonaws.com");
        List<String> ipPermissions = new ArrayList<String>();
        DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getAppName()));
        DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
        for (SecurityGroup group : result.getSecurityGroups())
            for (IpPermission perm : group.getIpPermissions())
                ipPermissions.addAll(perm.getIpRanges());

        return ipPermissions;
    }
}
