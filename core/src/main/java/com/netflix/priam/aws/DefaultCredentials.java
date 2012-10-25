package com.netflix.priam.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.inject.Singleton;
import com.netflix.priam.ICredential;

/**
 * First looks for environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY".  If not specified, next looks for
 * JVM system properties "aws.accessKeyId" and "aws.secretKey".  Falls back to instance profile credentials delivered
 * through the Amazon EC2 metadata service (the preferred approach for production).  Configure the latter via the IAM
 * API or AWS Management Console.
 */
@Singleton
public class DefaultCredentials implements ICredential {
    private final AWSCredentialsProvider _credentials = new DefaultAWSCredentialsProviderChain();

    @Override
    public AWSCredentialsProvider getCredentials() {
        return _credentials;
    }
}
