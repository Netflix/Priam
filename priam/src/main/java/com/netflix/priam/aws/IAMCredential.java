package com.netflix.priam.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.netflix.priam.ICredential;

public class IAMCredential implements ICredential
{
    private final InstanceProfileCredentialsProvider iamCredProvider;

    public IAMCredential()
    {
        this.iamCredProvider = new InstanceProfileCredentialsProvider();
    }

    public String getAccessKeyId()
    {
        return iamCredProvider.getCredentials().getAWSAccessKeyId();
    }

    public String getSecretAccessKey()
    {
        return iamCredProvider.getCredentials().getAWSSecretKey();
    }

    public AWSCredentials getCredentials()
    {
        return iamCredProvider.getCredentials();
    }

	public AWSCredentialsProvider getAwsCredentialProvider() 
	{
		return iamCredProvider;
	}
}
