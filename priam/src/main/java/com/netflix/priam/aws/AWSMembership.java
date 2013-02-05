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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.PriamInstance;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership
{
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final IConfiguration config;
    private final ICredential provider;    

    @Inject
    public AWSMembership(IConfiguration config, ICredential provider)
    {
        this.config = config;
        this.provider = provider;        
    }

    public List<PriamInstance> getAllInstances(String appName)
    {
        List<PriamInstance> instances = new LinkedList<PriamInstance>();

        //for now just get the ASGs in the current region
        for(String region : discoverRegions())
        {
            List<String> instanceIds = getLiveInstanceIds(region, deriveAsgs(appName));
            instances.addAll(getAllInstances(region, instanceIds));
        }

        return instances;
    }

    List<String> discoverRegions()
    {
        return Collections.singletonList("us-east-1");
    }

    List<String> deriveAsgs(String appName)
    {

        List<String> asgs = new LinkedList<String>();
        String baseAsgNAme = config.getASGName();

        //TODO: clean this logic up
        String curAsgZone = null;
        for(String r : config.getRacs())
        {
            if(baseAsgNAme.contains(r))
            {
                curAsgZone = r;
                break;
            }
        }

        for(String rac : config.getRacs())
        {
            asgs.add(baseAsgNAme.replaceAll(curAsgZone, rac));
//            asgs.add(appName + "--" + rac.replaceAll("\\-", ""));
        }
        logger.info(String.format("found ASGs for app %s = %s", appName, asgs.toString()));
        return asgs;
    }

    List<String> getLiveInstanceIds(String ec2Region, List<String> asgNames)
    {
        AmazonAutoScaling client = null;
        try
        {
            client = getAutoScalingClient(ec2Region);
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(asgNames);
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instances = Lists.newLinkedList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                logger.info(String.format("in region/asg %s/%s, found instances = ", ec2Region, asg.getAutoScalingGroupName(), asg.getInstances()));
                for (Instance ins : asg.getInstances())
                {
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated")))
                    {
                        instances.add(ins.getInstanceId());
                    }
                }
            }
            logger.info(String.format("found a total of %d live instances", instances.size()));
            return instances;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    List<PriamInstance> getAllInstances(String ec2Region, List<String> instanceIds)
    {
        DescribeInstancesRequest req = new DescribeInstancesRequest().withInstanceIds(instanceIds);
        DescribeInstancesResult res = getEc2Client(ec2Region).describeInstances(req);

        List<PriamInstance> realInstances = Lists.newLinkedList();
        logger.info("found reservations = " + res.getReservations());
        for(Reservation r : res.getReservations())
        {
            logger.info(String.format("reservation %s has these instances: %s", r.getReservationId(), r.getInstances()));
            for(com.amazonaws.services.ec2.model.Instance i : r.getInstances())
            {
                realInstances.add(new PriamInstance(i.getPublicDnsName(), i.getInstanceId(),
                        ec2Region, i.getPlacement().getAvailabilityZone(), i.getPublicIpAddress()));
            }
        }
        logger.info(String.format("expected instance cnt = %d, actual (via reservations) = %d", instanceIds.size(), realInstances.size()));
        return realInstances;
    }

    public PriamInstance getThisInstance()
    {
        return new PriamInstance(config);
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
            client = getAutoScalingClient(config.getDC());
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

    /**
     * Adds a iplist to the SG.
     */
    public void addACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client(config.getDC());
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
            logger.info("Done adding ACL to: " + StringUtils.join(listIPs, ","));
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
            client = getEc2Client(config.getDC());
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
            logger.info("Done removing from ACL: " + StringUtils.join(listIPs, ","));
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
            client = getEc2Client(config.getDC());
            List<String> ipPermissions = new ArrayList<String>();
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupName()));
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups())
                for (IpPermission perm : group.getIpPermissions())
                    if (perm.getFromPort() == from && perm.getToPort() == to)
                        ipPermissions.addAll(perm.getIpRanges());
            return ipPermissions;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    protected AmazonAutoScaling getAutoScalingClient(String  ec2Region)
    {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client(String ec2Region)
    {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}
