/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.identity;

import com.google.common.base.Preconditions;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;

/**
 * This class provides the central place to create and consume the identity of
 * the instance - token, seeds etc.
 * 
 */
@Singleton
public class InstanceIdentity
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);
    private static final String DUMMY_INSTANCE_ID = "new_slot";

    private final ListMultimap<String, PriamInstance> locMap = Multimaps.newListMultimap(new HashMap<String, Collection<PriamInstance>>(), new Supplier<List<PriamInstance>>()
    {
        public List<PriamInstance> get()
        {
            return Lists.newArrayList();
        }
    });

    private final IMembership membership;
    private final IConfiguration config;

    private final Predicate<PriamInstance> differentHostPredicate = new Predicate<PriamInstance>() {
    		@Override
    		public boolean apply(PriamInstance instance) {
    			return (!instance.getInstanceId().equalsIgnoreCase(DUMMY_INSTANCE_ID) && !instance.getHostName().equals(myInstance.getHostName()));
    		}
    };
   
    private PriamInstance myInstance;

    @Inject
    public InstanceIdentity(IMembership membership, IConfiguration config) throws Exception
    {
        this.membership = membership;
        this.config = config;
        myInstance = membership.getThisInstance();
    }

    public PriamInstance getInstance()
    {
        return myInstance;
    }

    private void populateRacMap()
    {
        locMap.clear();
        {
            locMap.put(ins.getRac(), ins);
        }
    }

    public List<String> getSeeds() throws UnknownHostException
    {
        populateRacMap();
        List<String> seeds = new LinkedList<String>();
        // Handle single zone deployment
        if (config.getRacs().size() == 1)
        {
            // Return empty list if all nodes are not up
            if (membership.getRacMembershipSize() != locMap.get(myInstance.getRac()).size())
                return seeds;
            // If seed node, return the next node in the list
            if (locMap.get(myInstance.getRac()).size() > 1 && locMap.get(myInstance.getRac()).get(0).getHostName().equals(myInstance.getHostName()))
                seeds.add(locMap.get(myInstance.getRac()).get(1).getHostName());
        }
        for (String loc : locMap.keySet())
        {
        		PriamInstance instance = Iterables.tryFind(locMap.get(loc), differentHostPredicate).orNull();
        		if (instance != null)
        			seeds.add(instance.getHostName());
        }
        return seeds;
    }

    public boolean isSeed()
    {
        populateRacMap();
        String ip = locMap.get(myInstance.getRac()).get(0).getHostName();
        return myInstance.getHostName().equals(ip);
    }
    
    public boolean isReplace(){
        return false;
    }
}
