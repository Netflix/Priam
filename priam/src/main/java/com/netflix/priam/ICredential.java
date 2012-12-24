package com.netflix.priam;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Credential file interface for services supporting 
 * Access ID and key authentication
 */
public interface ICredential
{
    /**
     * @return Access ID
     */
    public String getAccessKeyId();

    /**
     * @return Secret key
     */
    public String getSecretAccessKey();

    /**
     * Retrieve AWS credentials (id and key).
     * <p>
     * Added this method to handle potential data races in calling {code}getAccessKeyId{code}
     * and {code}getSecretAccessKey{code} sequentially.
     */
    @Deprecated
    AWSCredentials getCredentials();
    
    /**
     * Retrieve AWS Credential Provider object 
     */
    AWSCredentialsProvider getAwsCredentialProvider();
}
