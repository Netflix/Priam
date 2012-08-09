package com.netflix.priam.identity;

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;
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
 */
@Singleton
public class InstanceIdentity {
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);
    private final ListMultimap<String, PriamInstance> instancesByAvailabilityZoneMultiMap = Multimaps.newListMultimap(new HashMap<String, Collection<PriamInstance>>(), new Supplier<List<PriamInstance>>() {
        public List<PriamInstance> get() {
            return Lists.newArrayList();
        }
    });
    private final IPriamInstanceFactory factory;
    private final IMembership membership;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final Sleeper sleeper;

    private PriamInstance myInstance;
    private boolean isReplace = false;

    @Inject
    public InstanceIdentity(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, IPriamInstanceFactory factory, IMembership membership, Sleeper sleeper) throws Exception {
        this.factory = factory;
        this.membership = membership;
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.sleeper = sleeper;
        init();
    }

    public PriamInstance getInstance() {
        return myInstance;
    }

    public void init() throws Exception {
        // try to grab the token which was already assigned
        myInstance = new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                // Check if this node is decomissioned
                for (PriamInstance ins : factory.getAllIds(cassandraConfiguration.getClusterName() + "-dead")) {
                    logger.debug(String.format("Iterating though the hosts: %s", ins.getInstanceId()));
                    if (ins.getInstanceId().equals(amazonConfiguration.getInstanceID())) {
                        ins.setOutOfService(true);
                        return ins;
                    }
                }
                for (PriamInstance ins : factory.getAllIds(cassandraConfiguration.getClusterName())) {
                    logger.debug(String.format("Iterating though the hosts: %s", ins.getInstanceId()));
                    if (ins.getInstanceId().equals(amazonConfiguration.getInstanceID())) {
                        return ins;
                    }
                }
                return null;
            }
        }.call();
        // Grab a dead token
        if (null == myInstance) {
            myInstance = new GetDeadToken().call();
        }
        // Grab a new token
        if (null == myInstance) {
            myInstance = new GetNewToken().call();
        }
        logger.info("My token: " + myInstance.getToken());
    }

    private void populateInstancesByAvailabilityZoneMultiMap() {
        instancesByAvailabilityZoneMultiMap.clear();
        for (PriamInstance ins : factory.getAllIds(cassandraConfiguration.getClusterName())) {
            instancesByAvailabilityZoneMultiMap.put(ins.getAvailabilityZone(), ins);
        }
    }

    public class GetDeadToken extends RetryableCallable<PriamInstance> {
        @Override
        public PriamInstance retriableCall() throws Exception {
            final List<PriamInstance> allIds = factory.getAllIds(cassandraConfiguration.getClusterName());
            List<String> asgInstances = membership.getRacMembership();
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);
            for (PriamInstance dead : allIds) {
                // test same zone and is it is alive.
                if (!dead.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone()) || asgInstances.contains(dead.getInstanceId())) {
                    continue;
                }
                logger.info("Found dead instances: " + dead.getInstanceId());
                PriamInstance markAsDead = factory.create(dead.getApp() + "-dead", dead.getId(), dead.getInstanceId(), dead.getHostName(), dead.getHostIP(), dead.getAvailabilityZone(), dead.getVolumes(),
                        dead.getToken());
                // remove it as we marked it down...
                factory.delete(dead);
                isReplace = true;
                String payLoad = markAsDead.getToken();
                logger.info("Trying to grab slot {} with availability zone {}", markAsDead.getId(), markAsDead.getAvailabilityZone());
                return factory.create(cassandraConfiguration.getClusterName(), markAsDead.getId(), amazonConfiguration.getInstanceID(), amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(), markAsDead.getVolumes(), payLoad);
            }
            return null;
        }

        public void forEachExecution() {
            populateInstancesByAvailabilityZoneMultiMap();
        }
    }

    public class GetNewToken extends RetryableCallable<PriamInstance> {
        @Override
        public PriamInstance retriableCall() throws Exception {
            // Sleep random interval - upto 15 sec
            sleeper.sleep(new Random().nextInt(15000));
            int hash = TokenManager.regionOffset(amazonConfiguration.getRegionName());
            // use this hash so that the nodes are spred far away from the other
            // regions.

            int max = hash;
            for (PriamInstance data : factory.getAllIds(cassandraConfiguration.getClusterName())) {
                max = (data.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone()) && (data.getId() > max)) ? data.getId() : max;
            }
            int maxSlot = max - hash;
            int my_slot = 0;
            if (hash == max && instancesByAvailabilityZoneMultiMap.get(amazonConfiguration.getAvailabilityZone()).size() == 0) {
                my_slot = amazonConfiguration.getUsableAvailabilityZones().indexOf(amazonConfiguration.getAvailabilityZone()) + maxSlot;
            } else {
                my_slot = amazonConfiguration.getUsableAvailabilityZones().size() + maxSlot;
            }

            String payload = TokenManager.createToken(my_slot, membership.getRacCount(), membership.getAvailabilityZoneMembershipSize(), amazonConfiguration.getRegionName());
            return factory.create(cassandraConfiguration.getClusterName(), my_slot + hash, amazonConfiguration.getInstanceID(), amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(), null, payload);
        }

        public void forEachExecution() {
            populateInstancesByAvailabilityZoneMultiMap();
        }
    }

    public List<String> getSeeds() throws UnknownHostException {
        populateInstancesByAvailabilityZoneMultiMap();
        List<String> seeds = new LinkedList<String>();
        // Handle single zone deployment
        if (amazonConfiguration.getUsableAvailabilityZones().size() == 1) {
            // Return empty list if all nodes are not up
            if (membership.getAvailabilityZoneMembershipSize() != instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).size()) {
                return seeds;
            }
            // If seed node, return the next node in the list
            if (instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).size() > 1 && instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(0).getHostName().equals(myInstance.getHostName())) {
                seeds.add(instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(1).getHostName());
            }
        }
        logger.info("Retrieved seeds. My hostname: {}, AZ-To-Instance-MultiMap: {}", myInstance.getHostName(), instancesByAvailabilityZoneMultiMap);

        for (String loc : instancesByAvailabilityZoneMultiMap.keySet()) {
            seeds.add(instancesByAvailabilityZoneMultiMap.get(loc).get(0).getHostName());
        }

        //TODO: [mbogner] Removing this instance from the seed list seems odd and makes it difficult to test out a single node cluster during dev/test.  Commenting out for now.
        //seeds.remove(myInstance.getHostName());

        return seeds;
    }

    public boolean isSeed() {
        populateInstancesByAvailabilityZoneMultiMap();
        String seedHostNameForAvailabilityZone = instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(0).getHostName();
        return myInstance.getHostName().equals(seedHostNameForAvailabilityZone);
    }

    public boolean isReplace() {
        return isReplace;
    }
}
