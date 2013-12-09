package com.netflix.priam.backup;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.netflix.priam.ICredential;

public class FakeNullCredential implements ICredential
{
	public AWSCredentialsProvider getAwsCredentialProvider() {
		// TODO Auto-generated method stub
		return null;
	}
}
