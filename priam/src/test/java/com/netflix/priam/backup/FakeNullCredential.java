package com.netflix.priam.backup;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.netflix.priam.ICredential;

public class FakeNullCredential implements ICredential
{

    @Override
    public String getAccessKeyId()
    {
        return "testid";
    }

    @Override
    public String getSecretAccessKey()
    {
        return "testkey";
    }

    public AWSCredentials getCredentials()
    {
        return new BasicAWSCredentials(getAccessKeyId(), getSecretAccessKey());
    }

	@Override
	public AWSCredentialsProvider getAwsCredentialProvider() {
		// TODO Auto-generated method stub
		return null;
	}
}
