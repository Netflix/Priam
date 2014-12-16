package com.netflix.priam.identity.token;

import com.netflix.priam.identity.PriamInstance;

public interface IDeadTokenRetriever {

	public PriamInstance get();
}
