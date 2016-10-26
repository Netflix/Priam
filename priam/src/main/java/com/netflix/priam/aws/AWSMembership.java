/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import com.google.inject.name.Named;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.InstanceEnvIdentity;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership
{
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final IConfiguration config;
    private final ICredential provider;
	private final InstanceEnvIdentity insEnvIdentity;
	private final ICredential crossAccountProvider;

    @Inject
    public AWSMembership(IConfiguration config, ICredential provider,  @Named("awsec2roleassumption")ICredential crossAccountProvider, InstanceEnvIdentity insEnvIdentity)
    {
        this.config = config;
        this.provider = provider;  
        this.insEnvIdentity = insEnvIdentity;
        this.crossAccountProvider = crossAccountProvider;
    }

    @Override
    public List<String> getRacMembership()
    {
        AmazonAutoScaling client = null;
        try
        {
            List<String> asgNames = new ArrayList<>();
            asgNames.add(config.getASGName());
            asgNames.addAll(Arrays.asList(config.getSiblingASGNames().split("\\s*,\\s*")));
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(asgNames.toArray(new String[asgNames.size()]));
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                for (Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.getInstanceId());
            }
            logger.info(String.format("Querying Amazon returned following instance in the RAC: %s, ASGs: %s --> %s", config.getRac(),StringUtils.join(asgNames, ",") ,StringUtils.join(instanceIds, ",")));
            return instanceIds;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getRacMembershipSize()
    {
        AmazonAutoScaling client = null;
        try
        {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                size += asg.getMaxSize();
            }
            logger.info(String.format("Query on ASG returning %d instances", size));
            return size;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }
    
    @Override
    public List<String> getCrossAccountRacMembership()
    {
        AmazonAutoScaling client = null;
        try
        {
            List<String> asgNames = new ArrayList<>();
            asgNames.add(config.getASGName());
            asgNames.addAll(Arrays.asList(config.getSiblingASGNames().split("\\s*,\\s*")));
            client = getCrossAccountAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(asgNames.toArray(new String[asgNames.size()]));
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                for (Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.getInstanceId());
            }
            logger.info(String.format("Querying Amazon returned following instance in the cross-account ASG: %s --> %s", config.getRac(), StringUtils.join(instanceIds, ",")));
            return instanceIds;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    @Override
    public int getRacCount()
    {
        return config.getRacs().size();
    }

    /**
     * Adding peers' IPs as ingress to the running instance SG.  The running instance could be in "classic" or "vpc"
     */
    public void addACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            
            if (this.insEnvIdentity.isClassic()) {
                client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
                logger.info("Done adding ACL to classic: " + StringUtils.join(listIPs, ","));
            } else {
                AuthorizeSecurityGroupIngressRequest sgIngressRequest = new AuthorizeSecurityGroupIngressRequest();
                sgIngressRequest.withGroupId(getVpcGoupId()); //fetch SG group id for vpc account of the running instance.
                client.authorizeSecurityGroupIngress(sgIngressRequest.withIpPermissions(ipPermissions)); //Adding peers' IPs as ingress to the running instance SG
                logger.info("Done adding ACL to vpc: " + StringUtils.join(listIPs, ","));
            }
            

        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }
    
    /*
     * @return SG group id for a group name, vpc account of the running instance.
     */
    protected String getVpcGoupId()
    {
    	AmazonEC2 client = null;
    	try
    	{
    		client = getEc2Client();
    		Filter nameFilter = new Filter().withName("group-name").withValues(config.getACLGroupName()); //SG 
    		Filter vpcFilter = new Filter().withName("vpc-id").withValues(config.getVpcId());
    		
    		DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withFilters(nameFilter, vpcFilter);
    		DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
    		for (SecurityGroup group : result.getSecurityGroups())
    		{
    			logger.debug(String.format("got group-id:%s for group-name:%s,vpc-id:%s", group.getGroupId(), config.getACLGroupName(), config.getVpcId()));
    			return group.getGroupId();
    		}
    		logger.error(String.format("unable to get group-id for group-name=%s vpc-id=%s", config.getACLGroupName(), config.getVpcId()));
    		return "";
    	}
    	finally
    	{
    		if (client != null)
    			client.shutdown();
    	}
    }

    /**
     * removes a iplist from the SG
     */
    public void removeACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            
            if (this.insEnvIdentity.isClassic()) {
                client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
                logger.info("Done removing from ACL within classic env for running instance: " + StringUtils.join(listIPs, ","));            	
            } else {
            	RevokeSecurityGroupIngressRequest req = new RevokeSecurityGroupIngressRequest();
            	req.withGroupId(getVpcGoupId());  //fetch SG group id for vpc account of the running instance.
            	client.revokeSecurityGroupIngress(req.withIpPermissions(ipPermissions));  //Adding peers' IPs as ingress to the running instance SG
            	logger.info("Done removing from ACL within vpc env for running instance: " + StringUtils.join(listIPs, ","));
            }
            

        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL(int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<String>();
            
            if (this.insEnvIdentity.isClassic()) {
            	
                DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupName()));
                DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
                for (SecurityGroup group : result.getSecurityGroups())
                    for (IpPermission perm : group.getIpPermissions())
                        if (perm.getFromPort() == from && perm.getToPort() == to)
                            ipPermissions.addAll(perm.getIpRanges());
                
                logger.info("Fetch current permissions for classic env of running instance");
            } else {
            	
            	Filter nameFilter = new Filter().withName("group-name").withValues(config.getACLGroupName());
            	String vpcid = config.getVpcId();
            	if (vpcid == null || vpcid.isEmpty()) {
            		throw new IllegalStateException("vpcid is null even though instance is running in vpc.");
            	}
            		
            	Filter vpcFilter = new Filter().withName("vpc-id").withValues(vpcid); //only fetch SG for the vpc id of the running instance
            	DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withFilters(nameFilter, vpcFilter);
                DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
                for (SecurityGroup group : result.getSecurityGroups())
                    for (IpPermission perm : group.getIpPermissions())
                        if (perm.getFromPort() == from && perm.getToPort() == to)
                            ipPermissions.addAll(perm.getIpRanges());
                
                logger.info("Fetch current permissions for vpc env of running instance");
            }
            

            return ipPermissions;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    @Override
    public void expandRacMembership(int count)
    {
        AmazonAutoScaling client = null;
        try
        {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            AutoScalingGroup asg = res.getAutoScalingGroups().get(0);
            UpdateAutoScalingGroupRequest ureq = new UpdateAutoScalingGroupRequest();
            ureq.setAutoScalingGroupName(asg.getAutoScalingGroupName());
            ureq.setMinSize(asg.getMinSize() + 1);
            ureq.setMaxSize(asg.getMinSize() + 1);
            ureq.setDesiredCapacity(asg.getMinSize() + 1);
            client.updateAutoScalingGroup(ureq);
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    protected AmazonAutoScaling getAutoScalingClient()
    {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        return client;
    }
    
    protected AmazonAutoScaling getCrossAccountAutoScalingClient()
    {
        AmazonAutoScaling client = new AmazonAutoScalingClient(crossAccountProvider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client()
    {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}