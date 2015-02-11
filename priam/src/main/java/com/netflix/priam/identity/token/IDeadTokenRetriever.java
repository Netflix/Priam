package com.netflix.priam.identity.token;

import com.google.common.collect.ListMultimap;
import com.netflix.priam.identity.PriamInstance;

public interface IDeadTokenRetriever {

	public PriamInstance get() throws Exception;
	/*
	 * @return the IP address of the dead instance to which we will acquire its token
	 */
	public String getReplaceIp();
	/*
	 * @param A map of the rac for each instance.
	 */	
	public void setLocMap(ListMultimap<String, PriamInstance> locMap);
}
