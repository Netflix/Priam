package com.netflix.priam.identity;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;

/**
 * This class provides the central place to create and consume the identity of
 * the instance - token, seeds etc.
 * 
 */
@Singleton
public class InstanceIdentity
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);
    private ListMultimap<String, PriamInstance> locMap = Multimaps.newListMultimap(new HashMap<String, Collection<PriamInstance>>(), new Supplier<List<PriamInstance>>()
    {
        public List<PriamInstance> get()
        {
            return Lists.newArrayList();
        }
    });
    private IPriamInstanceFactory factory;
    private PriamInstance myInstance;
    private IMembership membership;
    private IConfiguration config;
    private boolean isReplace = false;

    @Inject
    public InstanceIdentity(IPriamInstanceFactory factory, IMembership membership, IConfiguration config) throws Exception
    {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
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
                    logger.debug(String.format("Iterating though the hosts: %s", ins.getInstanceId()));
                    if (ins.getInstanceId().equals(config.getInstanceName()))
                    {
                        ins.setOutOfService(true);
                        return ins;
                    }
                }
                for (PriamInstance ins : factory.getAllIds(config.getAppName()))
                {
                    logger.debug(String.format("Iterating though the hosts: %s", ins.getInstanceId()));
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
            Thread.sleep(new Random().nextInt(5000) + 10000);
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
            Thread.sleep(new Random().nextInt(15000));
            int hash = SystemUtils.hash(config.getDC());
            // use this hash so that the nodes are spred far away from the other
            // regions.

            int max = hash;
            for (PriamInstance data : factory.getAllIds(config.getAppName()))
                max = (data.getRac().equals(config.getRac()) && (data.getId() > max)) ? data.getId() : max;
            int maxSlot = max - hash;
            int my_slot = 0;
            if (hash == max && locMap.get(config.getRac()).size() == 0)
                my_slot = config.getRacs().indexOf(config.getRac()) + maxSlot;
            else
                my_slot = config.getRacs().size() + maxSlot;

            String payload = TokenManager.createToken(my_slot, membership.getRacCount(), membership.getRacMembershipSize(), config.getDC());
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
            seeds.add(locMap.get(loc).get(0).getHostName());
        seeds.remove(myInstance.getHostName());
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
