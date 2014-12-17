package com.netflix.priam.identity.token;

import com.netflix.priam.identity.PriamInstance;

public class TokenRetrieverBase {

	public static final String DUMMY_INSTANCE_ID = "new_slot";
	
    protected boolean isInstanceDummy(PriamInstance instance) 
    {
    	return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
}
