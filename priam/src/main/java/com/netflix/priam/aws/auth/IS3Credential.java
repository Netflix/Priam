package com.netflix.priam.aws.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.netflix.priam.ICredential;

/*
 * Credentials specific to Amazon S3
 */
public interface IS3Credential extends ICredential{

	public AWSCredentials getCredentials() throws Exception;
}
