package com.netflix.priam;

/**
 * Credential file interface for services supporting 
 * Access ID and key authentication for non-AWS
 */
public interface ICredentialGeneric extends ICredential {

	public byte[] getValue(KEY key);
	
	public static enum KEY {
		PGP_PUBLIC_KEY_LOC, PGP_PASSWORD, GCS_SERVICE_ID, GCS_PRIVATE_KEY_LOC
	}
}
