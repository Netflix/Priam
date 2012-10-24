package com.netflix.priam.defaultimpl;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.netflix.priam.ICredential;

/**
 * Loads credentials from the Amazon EC2 Instance Metadata Service.  Configure
 * the permissions via the IAM API or AWS Management Console.
 */
public class InstanceProfileCredential implements ICredential {

    private static final InstanceProfileCredentialsProvider provider =
            new InstanceProfileCredentialsProvider();

    @Override
    public AWSCredentialsProvider getCredentials() {
        return provider;
    }
}
