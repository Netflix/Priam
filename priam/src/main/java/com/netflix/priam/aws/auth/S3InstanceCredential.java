package com.netflix.priam.aws.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

/*
 * Provides credentials from the S3 instance.
 */
public class S3InstanceCredential implements IS3Credential {

	private InstanceProfileCredentialsProvider credentialsProvider;

	public S3InstanceCredential() {
		this.credentialsProvider = new InstanceProfileCredentialsProvider(); 
	}
	
	@Override
	public AWSCredentials getCredentials() throws Exception {
		return this.credentialsProvider.getCredentials();
	}

	@Override
	public AWSCredentialsProvider getAwsCredentialProvider() {
		return this.credentialsProvider;
	}
	
	

}