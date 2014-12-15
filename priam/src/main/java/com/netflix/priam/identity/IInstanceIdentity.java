package com.netflix.priam.identity;

public interface IInstanceIdentity {

	public boolean isReplace();
	public boolean isTokenPregenerated();
	public String getReplacedIp();
}
