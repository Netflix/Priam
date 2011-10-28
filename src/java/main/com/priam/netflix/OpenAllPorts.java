package com.priam.netflix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.IpPermission;
import com.netflix.aws.AWSManager;

public class OpenAllPorts extends ValidateInstanceID
{
    public static void main(String[] args) throws Exception
    {
        init();
        AWSCredentials cred = new BasicAWSCredentials(AWSManager.getInstance().getAccessKeyId(), AWSManager.getInstance().getSecretAccessKey());
        AmazonEC2 client = new AmazonEC2Client(cred);
        client.setEndpoint("ec2." + REGION + ".amazonaws.com");
        List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
        IpPermission ip = new IpPermission();
        ip.setFromPort(0);
        ip.setIpProtocol("tcp");
        ip.setIpRanges(Arrays.asList("0.0.0.0/0"));
        ip.setToPort(65535);
        ipPermissions.add(ip);
        AuthorizeSecurityGroupIngressRequest req = new AuthorizeSecurityGroupIngressRequest(APP_NAME, ipPermissions);
        client.authorizeSecurityGroupIngress(req);
    }

}
