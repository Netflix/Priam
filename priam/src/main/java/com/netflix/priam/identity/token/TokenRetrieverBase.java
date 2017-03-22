/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
