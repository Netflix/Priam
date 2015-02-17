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
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.token.IDeadTokenRetriever;
import com.netflix.priam.identity.token.INewTokenRetriever;
import com.netflix.priam.identity.token.IPreGeneratedTokenRetriever;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.ws.rs.core.MediaType;

/**
 * This class provides the central place to create and consume the identity of
 * the instance - token, seeds etc.
 * 
 */
@Singleton
public class InstanceIdentity
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);
    public static final String DUMMY_INSTANCE_ID = "new_slot";

    private final ListMultimap<String, PriamInstance> locMap = Multimaps.newListMultimap(new HashMap<String, Collection<PriamInstance>>(), new Supplier<List<PriamInstance>>()
    {
        public List<PriamInstance> get()
        {
            return Lists.newArrayList();
        }
    });
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private final ITokenManager tokenManager;

    private final Predicate<PriamInstance> differentHostPredicate = new Predicate<PriamInstance>() {
    		@Override
    		public boolean apply(PriamInstance instance) {
    			return (!instance.getInstanceId().equalsIgnoreCase(DUMMY_INSTANCE_ID) && !instance.getHostName().equals(myInstance.getHostName()));
    		}
    };
   
    private PriamInstance myInstance;
    private boolean isReplace = false;
    private boolean isTokenPregenerated = false;
    private String replacedIp = "";
	private IDeadTokenRetriever deadTokenRetriever;
	private IPreGeneratedTokenRetriever preGeneratedTokenRetriever;
	private INewTokenRetriever newTokenRetriever;

    @Inject
    //Note: do not parameterized the generic type variable to an implementation as it confuses Guice in the binding.
    public InstanceIdentity(IPriamInstanceFactory factory, IMembership membership, IConfiguration config,
            Sleeper sleeper, ITokenManager tokenManager
            , IDeadTokenRetriever deadTokenRetriever
            , IPreGeneratedTokenRetriever preGeneratedTokenRetriever
            , INewTokenRetriever newTokenRetriever
            ) throws Exception
    {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.sleeper = sleeper;
        this.tokenManager = tokenManager;
        this.deadTokenRetriever = deadTokenRetriever;
        this.preGeneratedTokenRetriever = preGeneratedTokenRetriever;
        this.newTokenRetriever = newTokenRetriever;
        init();
    }

    public PriamInstance getInstance()
    {
        return myInstance;
    }

    public void init() throws Exception
    {
        // try to grab the token which was already assigned
        myInstance = new RetryableCallable<PriamInstance>()
        {
            @Override
            public PriamInstance retriableCall() throws Exception
            {
                // Check if this node is decomissioned
            	List<PriamInstance> deadInstances = factory.getAllIds(config.getAppName() + "-dead");
                for (PriamInstance ins : deadInstances)
                {
                    logger.info(String.format("[Dead] Iterating though the hosts: %s", ins.getInstanceId()));
                    if (ins.getInstanceId().equals(config.getInstanceName()))
                    {
                        ins.setOutOfService(true);
                        logger.info("[Dead]  found that this node is dead."
                        		+ " application: " + ins.getApp()
                        		+ ", id: " + ins.getId()
                        		+ ", intsance: " + ins.getInstanceId()
                        		+ ", region: " + ins.getDC()
                        		+ ", host ip: " + ins.getHostIP()
                        		+ ", host name: " + ins.getHostName()
                        		+ ", token: " + ins.getToken()
                        		);
                        return ins; 
                    }
                }
                List<PriamInstance> aliveInstances = factory.getAllIds(config.getAppName());
                for (PriamInstance ins : aliveInstances)
                {
                    logger.info(String.format("[Alive] Iterating though the hosts: %s My id = [%s]", ins.getInstanceId(),ins.getId()));
                    if (ins.getInstanceId().equals(config.getInstanceName())) {
                        logger.info("[Alive]  found that this node is alive."
                        		+ " application: " + ins.getApp()
                        		+ ", id: " + ins.getId()
                        		+ ", instance: " + ins.getInstanceId()
                        		+ ", region: " + ins.getDC()
                        		+ ", host ip: " + ins.getHostIP()
                        		+ ", host name: " + ins.getHostName()
                        		+ ", token: " + ins.getToken()
                        		);                    	
                    	return ins;
                    }
                        
                }
                return null;
            }
        }.call();

        // Grab a dead token
        if (null == myInstance) {
        	myInstance = new RetryableCallable<PriamInstance>() {
        		
        		@Override
                public PriamInstance retriableCall() throws Exception {
        			PriamInstance result = null;
        			result = deadTokenRetriever.get();
        			if (result != null) {
        				
        				isReplace = true; //indicate that we are acquiring a dead instance's token
        				
        				if (deadTokenRetriever.getReplaceIp() != null) { //The IP address of the dead instance to which we will acquire its token
            				replacedIp = deadTokenRetriever.getReplaceIp();        					
        				}
        				
        			}
        			
        			return result; 
        		}
        		
        		@Override
                public void forEachExecution()
                {
                    populateRacMap();
                    deadTokenRetriever.setLocMap(locMap);
                } 
        		
        	}.call();
        }

        
        // Grab a pre-generated token if there is such one
        if (null == myInstance) {

            myInstance = new RetryableCallable<PriamInstance>() {
            	
                @Override
                public PriamInstance retriableCall() throws Exception {
                	PriamInstance result = null;
                	result = preGeneratedTokenRetriever.get();
                	if (result != null) {
                		isTokenPregenerated = true;
                	}
                	return result;
                }
                
                @Override
                public void forEachExecution()
                {
                    populateRacMap();
                    preGeneratedTokenRetriever.setLocMap(locMap);
                }                

            }.call();
        	
        }

        	
        // Grab a new token
        if (null == myInstance)
        {
			
        	if ( this.config.isCreateNewTokenEnable() ) {

    			myInstance = new RetryableCallable<PriamInstance>() {
    				
    		        @Override
    		        public PriamInstance retriableCall() throws Exception {
    		        	super.set(100, 100);
    		        	newTokenRetriever.setLocMap(locMap);
    		        	return newTokenRetriever.get();
    		        }
    		        
    		        @Override
    		        public void forEachExecution()
    		        {
    		            populateRacMap();
    		            newTokenRetriever.setLocMap(locMap);
    		        }		        
    				
    			}.call();        		
        		
        	} else {
        		throw new IllegalStateException("Node attempted to erroneously create a new token when we should be grabbing an existing token.");
        	}			
			
		}
        
        logger.info("My token: " + myInstance.getToken());
    }

    private void populateRacMap()
    {
        locMap.clear();
        List<PriamInstance> instances = factory.getAllIds(config.getAppName()); 
        for (PriamInstance ins : instances)
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
            if (locMap.get(myInstance.getRac()).size() > 1 && locMap.get(myInstance.getRac()).get(0).getHostIP().equals(myInstance.getHostIP()))
            {	
            	PriamInstance instance = locMap.get(myInstance.getRac()).get(1);
            	if (instance != null && !isInstanceDummy(instance))
            	{
            	    if (config.isMultiDC())
            		   seeds.add(instance.getHostIP());
            	    else 
            		   seeds.add(instance.getHostName());
                }
            }
        }
        for (String loc : locMap.keySet())
        {
        		PriamInstance instance = Iterables.tryFind(locMap.get(loc), differentHostPredicate).orNull();
        		if (instance != null && !isInstanceDummy(instance))
        		{
        			if (config.isMultiDC())
        			   seeds.add(instance.getHostIP());
        			else
        			   seeds.add(instance.getHostName());
        		}
        }
        return seeds;
    }
    
    public boolean isSeed()
    {
        populateRacMap();
        String ip = locMap.get(myInstance.getRac()).get(0).getHostName();
        return myInstance.getHostName().equals(ip);
    }
    
    public boolean isReplace() 
    {
        return isReplace;
    }
    public boolean isTokenPregenerated()
    {
    	return isTokenPregenerated;
    }
    public String getReplacedIp()
    {
    	return replacedIp;
    }
    
    private static boolean isInstanceDummy(PriamInstance instance) 
    {
    	return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
}