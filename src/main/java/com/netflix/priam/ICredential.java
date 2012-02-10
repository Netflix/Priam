package com.netflix.priam;

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

}
