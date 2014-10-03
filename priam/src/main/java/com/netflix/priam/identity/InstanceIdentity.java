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
import com.netflix.priam.backup.Restore;
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

    private final ListMultimap<DcAndRac, PriamInstance> locMap = Multimaps.newListMultimap(new HashMap<DcAndRac, Collection<PriamInstance>>(), new Supplier<List<PriamInstance>>()
    {
        public List<PriamInstance> get()
        {
            return Lists.newArrayList();
        }
    });
    private final IPriamInstanceFactory factory;
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

    @Inject
    public InstanceIdentity(IPriamInstanceFactory factory, IMembership membership, IConfiguration config,
            Sleeper sleeper, ITokenManager tokenManager) throws Exception
    {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.sleeper = sleeper;
        this.tokenManager = tokenManager;
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
                for (PriamInstance ins : factory.getAllIds(config.getAppName() + "-dead"))
                {
                    logger.debug(String.format("[Dead] Iterating though the hosts: %s", ins.getInstanceId()));
                    if (ins.getInstanceId().equals(config.getInstanceName()))
                    {
                        ins.setOutOfService(true);
                        return ins;
                    }
                }
                for (PriamInstance ins : factory.getAllIds(config.getAppName()))
                {
                    logger.debug(String.format("[Alive] Iterating though the hosts: %s My id = [%s]", ins.getInstanceId(),ins.getId()));
                    if (ins.getInstanceId().equals(config.getInstanceName()))
                        return ins;
                }
                return null;
            }
        }.call();
        // Grab a dead token
        if (null == myInstance)
            myInstance = new GetDeadToken().call();
        
        // Grab a pre-generated token if there is such one
        if (null == myInstance)
           myInstance = new GetPregeneratedToken().call();
        	
        // Grab a new token
        if (null == myInstance)
        {
			GetNewToken newToken = new GetNewToken();
			newToken.set(100, 100);
			myInstance = newToken.call();
		}
        logger.info("My token: " + myInstance.getToken());
        
    }

    private void populateRacMap()
    {
        locMap.clear();
        for (PriamInstance ins : factory.getAllIds(config.getAppName()))
        {
        		locMap.put(new DcAndRac(ins.getDC(), ins.getRac()), ins);
        }
    }

    public class GetDeadToken extends RetryableCallable<PriamInstance>
    {
        @Override
        public PriamInstance retriableCall() throws Exception
        {
        	logger.info("Looking for a token from any dead node");
            final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
            List<String> asgInstances = membership.getRacMembership();
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);
            for (PriamInstance dead : allIds)
            {
                // test same zone and is it is alive.
                if (!dead.getDC().equals(config.getDC()) || !dead.getRac().equals(config.getRac()) || asgInstances.contains(dead.getInstanceId()) || isInstanceDummy(dead))
                    continue;
                logger.info("Found dead instances: " + dead.getInstanceId());
                PriamInstance markAsDead = factory.create(dead.getApp() + "-dead", dead.getId(), dead.getInstanceId(), dead.getHostName(), dead.getHostIP(), dead.getRac(), dead.getVolumes(),
                        dead.getToken());
                // remove it as we marked it down...
                factory.delete(dead);
                
                if (!Restore.isRestoreEnabled(config)) {
                   isReplace = true;
                   //find the replaced IP
                   replacedIp = findReplaceIp(allIds, markAsDead.getToken(), markAsDead.getDC());
                   if (replacedIp == null)
                      replacedIp = markAsDead.getHostIP();
                } else {
                    isReplace = false;
                }
                
                String payLoad = markAsDead.getToken();
                logger.info("Trying to grab slot {} with availability zone {}", markAsDead.getId(), markAsDead.getRac());
                return factory.create(config.getAppName(), markAsDead.getId(), config.getInstanceName(), config.getHostname(), config.getHostIP(), config.getRac(), markAsDead.getVolumes(), payLoad);
                
            }
            return null;
        }

        public void forEachExecution()
        {
            populateRacMap();
        }
        
        private String findReplaceIp(List<PriamInstance> allIds, String token, String location)
        {
            String ip = null;
            for (PriamInstance ins : allIds) {
                logger.info("Calling getIp on hostname[" + ins.getHostName() + "] and token[" + token + "]");
                if (ins.getToken().equals(token) || !ins.getDC().equals(location)) { //avoid using dead instance and other regions' instances
                    continue;	
                }
                
          	    try {
        	       ip = getIp(ins.getHostName(), token);
        	    } catch (ParseException e) {
                   ip = null;
                }
        		
        	    if (ip != null) {
                    logger.info("Found the IP: " + ip);
                    return ip;
                }
            }
        	
            return null;
        }
        
        private String getBaseURI(String host) 
        {
               return "http://" + host + ":8080/";
	    }
		
        private String getIp(String host, String token) throws ParseException 
	    {
               ClientConfig config = new DefaultClientConfig();
	           Client client = Client.create(config);
               WebResource service = client.resource(getBaseURI(host));
		    
               ClientResponse clientResp;
               String textEntity = null;
               
               try {
                  clientResp = service.path("Priam/REST/v1/cassadmin/gossipinfo").accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);	
		    
                  if (clientResp.getStatus() != 200)
		             return null;
		    
                  textEntity = clientResp.getEntity(String.class);
			
	              logger.info("Respond from calling gossipinfo on host[" + host + "] and token[" + token + "] : " + textEntity);
	              
	              if (StringUtils.isEmpty(textEntity))
	                  return null;
               } catch (Exception e) {
                   logger.debug("Error in reaching out to host: " + getBaseURI(host));
                   return null;
               }
			
	           JSONParser parser = new JSONParser();
               Object obj = parser.parse(textEntity);
			
               JSONObject jsonObject = (JSONObject) obj;
			
               Iterator iter = jsonObject.keySet().iterator();
			
               while (iter.hasNext()) {
                    Object key = iter.next();
                    JSONObject msg = (JSONObject) jsonObject.get(key);
                    if (msg.get("  STATUS") == null) {
                        continue;
                    }
		            String statusVal = (String) msg.get("  STATUS");
                    String[] ss = statusVal.split(",");
                
                    if (ss[1].equals(token)) {
                       return (String) key;
                    }
                
              }
              return null;
        }
    }
    
    
    public class GetPregeneratedToken extends RetryableCallable<PriamInstance>
    {
        @Override
        public PriamInstance retriableCall() throws Exception
        {
        	logger.info("Looking for any pre-generated token");
            final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
            List<String> asgInstances = membership.getRacMembership();
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);
            for (PriamInstance dead : allIds)
            {
                // test same zone and is it is alive.
                if (!dead.getDC().equals(config.getDC()) || !dead.getRac().equals(config.getRac()) || asgInstances.contains(dead.getInstanceId()) || !isInstanceDummy(dead))
                    continue;
                logger.info("Found pre-generated token: " + dead.getToken());
                PriamInstance markAsDead = factory.create(dead.getApp() + "-dead", dead.getId(), dead.getInstanceId(), dead.getHostName(), dead.getHostIP(), dead.getRac(), dead.getVolumes(),
                        dead.getToken());
                // remove it as we marked it down...
                factory.delete(dead);
                isTokenPregenerated = true;
           
                String payLoad = markAsDead.getToken();
                logger.info("Trying to grab slot {} with availability zone {}", markAsDead.getId(), markAsDead.getRac());
                return factory.create(config.getAppName(), markAsDead.getId(), config.getInstanceName(), config.getHostname(), config.getHostIP(), config.getRac(), markAsDead.getVolumes(), payLoad);
            }
            return null;
        }

        public void forEachExecution()
        {
            populateRacMap();
        }
    }
    

    public class GetNewToken extends RetryableCallable<PriamInstance>
    {
        @Override
        public PriamInstance retriableCall() throws Exception
        {
        	logger.info("Generating my own and new token");
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(15000));
            int hash = tokenManager.dcOffset(config.getDC());
            // use this hash so that the nodes are spred far away from the other
            // regions.

            int max = hash;
            for (PriamInstance data : factory.getAllIds(config.getAppName()))
                max = (data.getDC().equals(config.getDC()) && data.getRac().equals(config.getRac()) && (data.getId() > max)) ? data.getId() : max;
            int maxSlot = max - hash;
            int my_slot = 0;
            if (hash == max && locMap.get(new DcAndRac(config.getDC(), config.getRac())).size() == 0) {
                int idx = config.getRacs().indexOf(config.getRac());
                Preconditions.checkState(idx >= 0, "Rac %s is not in Racs %s", config.getRac(), config.getRacs());
                my_slot = idx + maxSlot;
            } else
                my_slot = config.getRacs().size() + maxSlot;

            logger.info(String.format("Trying to createToken with slot %d with rac count %d with rac membership size %d with dc %s",
                    my_slot, membership.getRacCount(), membership.getRacMembershipSize(), config.getDC()));
            String payload = tokenManager.createToken(my_slot, membership.getRacCount(), membership.getRacMembershipSize(), config.getDC());
            return factory.create(config.getAppName(), my_slot + hash, config.getInstanceName(), config.getHostname(), config.getHostIP(), config.getRac(), null, payload);
        }

        public void forEachExecution()
        {
            populateRacMap();
        }
    }

    public List<String> getSeeds() throws UnknownHostException
    {
        populateRacMap();
        List<String> seeds = new LinkedList<String>();

        DcAndRac myDcAndRac = new DcAndRac(myInstance.getDC(), myInstance.getRac());

        // Handle single zone deployment
        if (config.getRacs().size() == 1)
        {
            // Return empty list if all nodes are not up
            if (membership.getRacMembershipSize() != locMap.get(myDcAndRac).size())
                return seeds;
            // If seed node, return the next node in the list
            if (locMap.get(myDcAndRac).size() > 1 && locMap.get(myDcAndRac).get(0).getHostIP().equals(myInstance.getHostIP()))
            {	
            	PriamInstance instance = locMap.get(myDcAndRac).get(1);
            	if (instance != null && !isInstanceDummy(instance))
            	{
            	    if (config.isMultiDC())
            		   seeds.add(instance.getHostIP());
            	    else 
            		   seeds.add(instance.getHostName());
                }
            }
        }
        for (DcAndRac loc : locMap.keySet())
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
        String ip = locMap.get(new DcAndRac(myInstance.getDC(), myInstance.getRac())).get(0).getHostName();
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
    
    private boolean isInstanceDummy(PriamInstance instance) 
    {
    	return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }

    public static class DcAndRac
    {
        private final String dc;
        private final String rac;

        public DcAndRac(String dc, String rac)
        {
            this.dc = dc;
            this.rac = rac;
        }

        public String getDc()
        {
            return dc;
        }

        public String getRac()
        {
            return rac;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DcAndRac dcAndRac = (DcAndRac) o;

            if (!dc.equals(dcAndRac.dc)) return false;
            if (!rac.equals(dcAndRac.rac)) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = dc.hashCode();
            result = 31 * result + rac.hashCode();
            return result;
        }
    }
}
