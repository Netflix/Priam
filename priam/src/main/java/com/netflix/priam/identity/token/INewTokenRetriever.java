package com.netflix.priam.identity.token;

import com.google.common.collect.ListMultimap;
import com.netflix.priam.identity.PriamInstance;

public interface INewTokenRetriever {

	public PriamInstance get() throws Exception;
	/*
	 * @param A map of the rac for each instance.
	 */
	public void setLocMap(ListMultimap<String, PriamInstance> locMap);
}
