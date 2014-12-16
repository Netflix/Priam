package com.netflix.priam.identity;

/*
 * A means to create and consume the identity of the instance - token, seeds etc.
 */
public interface IInstanceIdentity {

	public boolean isReplace();
	public boolean isTokenPregenerated();
	public String getReplacedIp();
}
