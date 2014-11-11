package com.netflix.priam.aws.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

/*
 * Credentials specific to Amazon S3
 */
public interface IS3Credential {

	public AWSCredentials getCredentials() throws Exception;
	public AWSCredentialsProvider getCredentialsProvider() throws Exception;
	
}
