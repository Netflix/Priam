package com.netflix.priam.identity.token;

import com.netflix.priam.identity.PriamInstance;

public class PreGeneratedTokenRetriever implements IPreGeneratedTokenRetriever {

	@Override
	public PriamInstance get() {
		throw new UnsupportedOperationException();
	}

}
