package com.netflix.priam.identity.token;

import com.netflix.priam.identity.PriamInstance;

public class DeadTokenRetriever implements IDeadTokenRetriever {

	@Override
	public PriamInstance get() {
		throw new UnsupportedOperationException();
	}

}
