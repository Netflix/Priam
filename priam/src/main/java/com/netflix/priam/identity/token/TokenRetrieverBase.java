package com.netflix.priam.identity.token;

import java.util.Random;

import com.netflix.priam.identity.PriamInstance;

public class TokenRetrieverBase {

	public static final String DUMMY_INSTANCE_ID = "new_slot";
	private static final int MAX_VALUE_IN_MILISECS = 300000; //sleep up to 5 minutes
	protected Random randomizer;
	
	public TokenRetrieverBase() {
		this.randomizer = new Random();
	}
	
    protected boolean isInstanceDummy(PriamInstance instance) 
    {
    	return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
    
    /*
     * Return a random time for a thread to sleep. 
     * 
     * @return time in millisecs
     */
    protected long getSleepTime() {
    	return (long) this.randomizer.nextInt(MAX_VALUE_IN_MILISECS);
    }
}
