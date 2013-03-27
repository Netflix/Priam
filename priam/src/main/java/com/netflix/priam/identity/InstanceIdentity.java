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
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

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
        // Grab a new token
        if (null == myInstance)
            myInstance = new GetNewToken().call();
        logger.info("My token: " + myInstance.getToken());
    }

    private void populateRacMap()
    {
        locMap.clear();
        for (PriamInstance ins : factory.getAllIds(config.getAppName()))
        {
        		locMap.put(ins.getRac(), ins);
        }
    }

    public class GetDeadToken extends RetryableCallable<PriamInstance>
    {
        @Override
        public PriamInstance retriableCall() throws Exception
        {
            final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
            List<String> asgInstances = membership.getRacMembership();
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);
            for (PriamInstance dead : allIds)
            {
                // test same zone and is it is alive.
                if (!dead.getRac().equals(config.getRac()) || asgInstances.contains(dead.getInstanceId()))
                    continue;
                logger.info("Found dead instances: " + dead.getInstanceId());
                PriamInstance markAsDead = factory.create(dead.getApp() + "-dead", dead.getId(), dead.getInstanceId(), dead.getHostName(), dead.getHostIP(), dead.getRac(), dead.getVolumes(),
                        dead.getToken());
                // remove it as we marked it down...
                factory.delete(dead);
                isReplace = true;
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
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(15000));
            int hash = tokenManager.regionOffset(config.getDC());
            // use this hash so that the nodes are spred far away from the other
            // regions.

            int max = hash;
            for (PriamInstance data : factory.getAllIds(config.getAppName()))
                max = (data.getRac().equals(config.getRac()) && (data.getId() > max)) ? data.getId() : max;
            int maxSlot = max - hash;
            int my_slot = 0;
            if (hash == max && locMap.get(config.getRac()).size() == 0) {
                int idx = config.getRacs().indexOf(config.getRac());
                Preconditions.checkState(idx >= 0, "Rac %s is not in Racs %s", config.getRac(), config.getRacs());
                my_slot = idx + maxSlot;
            } else
                my_slot = config.getRacs().size() + maxSlot;

            logger.info("Trying to createToken with slot {} with rac count {} with rac membership size {} with dc {}",
                    my_slot, membership.getRacCount(), membership.getRacMembershipSize(), config.getDC());
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
        return isReplace;
    }
}
