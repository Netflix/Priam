package com.netflix.priam;

import com.amazonaws.auth.AWSCredentials;

/**
 * Credential file interface for services supporting 
 * Access ID and key authentication
 */
public interface ICredential
{
    /**
     * @return Access ID
     * @deprecated See {code}getCredentials{code}
     */
    @Deprecated
    public String getAccessKeyId();

    /**
     * @return Secret key
     * @deprecated See {code}getCredentials{code}
     */
    @Deprecated
    public String getSecretAccessKey();

    /**
     * Retrieve AWS credentials (id and key).
     * <p>
     * Added this method to handle potential data races in calling {code}getAccessKeyId{code}
     * and {code}getSecretAccessKey{code} sequentially.
     */
    AWSCredentials getCredentials();
}
