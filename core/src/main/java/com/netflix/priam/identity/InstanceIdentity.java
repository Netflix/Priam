package com.netflix.priam.identity;

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
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
    private final IPriamInstanceRegistry instanceRegistry;
    private final IMembership membership;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final TokenManager tokenManager;
    private final Sleeper sleeper;

    private PriamInstance myInstance;
    private boolean isReplace = false;

    @Inject
    public InstanceIdentity(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration,
                            IPriamInstanceRegistry instanceRegistry, IMembership membership, TokenManager tokenManager, Sleeper sleeper) throws Exception {
        this.instanceRegistry = instanceRegistry;
        this.membership = membership;
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.tokenManager = tokenManager;
        this.sleeper = sleeper;
        init();
    }

    public PriamInstance getInstance() {
        return myInstance;
    }

    public void init() throws Exception {
        // try to grab the token which was already assigned
        myInstance = new GetOwnToken().call();

        // If no token has already been assigned to this instance, grab a token that belonged to an instance that is no longer present
        if (null == myInstance) {
            myInstance = new GetDeadToken().call();
        }

        // If no token has already been assigned, and there are no dead tokens to resurrect, allocate a new token
        if (null == myInstance) {
            myInstance = new GetNewToken().call();
        }

        logger.info("My token: " + myInstance.getToken());
    }

    public class GetOwnToken extends RetryableCallable<PriamInstance> {
        @Override
        public PriamInstance retriableCall() throws Exception {
            // Look to see if an instance with the same instanceID is already part of the cluster.  If so, use it.
            for (PriamInstance ins : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
                logger.debug(String.format("Iterating through the hosts: %s", ins.getInstanceId()));
                if (ins.getInstanceId().equals(amazonConfiguration.getInstanceID())) {
                    return ins;
                }
            }
            return null;
        }

        public void forEachExecution() {
            populateInstancesByAvailabilityZoneMultiMap();
        }
    }

    public class GetDeadToken extends RetryableCallable<PriamInstance> {
        @Override
        public PriamInstance retriableCall() throws Exception {
            final List<PriamInstance> priamInstances = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName());
            List<String> asgInstanceIDs = membership.getAutoScaleGroupMembership();
            // Sleep random interval - 10 to 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);
            for (final PriamInstance deadInstance : priamInstances) {
                // Only consider instances that are in the same availability zone but not in the auto-scale group
                if (!deadInstance.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone())
                        || asgInstanceIDs.contains(deadInstance.getInstanceId())) {
                    // This instance isn't really dead.
                    continue;
                }

                // If we're here, it means we had a record in our instance registry pointing to something in our same availability zone, but that isn't
                // a current part of our ASG.  This would normally mean the node has died.

                logger.info("Found dead instance {} with token {}.", deadInstance.getInstanceId(), deadInstance.getToken());
                instanceRegistry.delete(deadInstance);

                logger.info("Trying to grab slot {} in availability zone {} with token {}", new Object[]{deadInstance.getId(), deadInstance.getAvailabilityZone(), deadInstance.getToken()});
                isReplace = true;
                return instanceRegistry.create(
                        cassandraConfiguration.getClusterName(),
                        deadInstance.getId(),
                        amazonConfiguration.getInstanceID(),
                        amazonConfiguration.getPrivateHostName(),
                        amazonConfiguration.getPrivateIP(),
                        amazonConfiguration.getAvailabilityZone(),
                        deadInstance.getVolumes(),
                        deadInstance.getToken());
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

            // use this hash so that the nodes are spread far away from the other regions.
            int max = hash;

            /*
            A PriamInstance's id is the same as it's owning region's hash + an index counter.  This is different from the token assignment.
            For example:
                 - the hash for "us-east-1" is 1808575600
                 - the "id" for the first instance in that region is 1808575600
                 - the "id" for the second instance in that region is 1808575601
                 - the "id" for the third instance in that region is 1808575602
                 - and so on...
            */

            // Iterate over all nodes in the cluster in the same availability zone and find the max "id"
            for (PriamInstance priamInstance : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
                if (priamInstance.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone())
                        && (priamInstance.getId() > max)) {
                    max = priamInstance.getId();
                }
            }

            /*
            If the following instances started, this is how their slots would be calculated:
            us-east-1a1   max = 1808575600, maxSlot = 0 =====> mySlot = 0, id = 1808575600
            us-east-1a2   max = 1808575600, maxSlot = 0 =====> mySlot = 3, id = 1808575603
            us-east-1b1   max = 1808575600, maxSlot = 0 =====> mySlot = 1, id = 1808575601
            us-east-1b2   max = 1808575600, maxSlot = 1 =====> mySlot = 4, id = 1808575604
            us-east-1c1   max = 1808575600, maxSlot = 0 =====> mySlot = 2, id = 1808575602
            us-east-1c2   max = 1808575600, maxSlot = 2 =====> mySlot = 5, id = 1808575605
             */

            int maxSlot = max - hash;
            int mySlot = 0;
            if (hash == max && instancesByAvailabilityZoneMultiMap.get(amazonConfiguration.getAvailabilityZone()).size() == 0) {
                // This is the first instance in the region and first instance in its availability zone.
                mySlot = amazonConfiguration.getUsableAvailabilityZones().indexOf(amazonConfiguration.getAvailabilityZone()) + maxSlot;
            } else {
                mySlot = amazonConfiguration.getUsableAvailabilityZones().size() + maxSlot;
            }

            String token = tokenManager.createToken(mySlot, membership.getUsableAvailabilityZones(), membership.getAvailabilityZoneMembershipSize(), amazonConfiguration.getRegionName());
            return instanceRegistry.create(cassandraConfiguration.getClusterName(), mySlot + hash, amazonConfiguration.getInstanceID(), amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(), null, token);
        }

        public void forEachExecution() {
            populateInstancesByAvailabilityZoneMultiMap();
        }
    }

    private void populateInstancesByAvailabilityZoneMultiMap() {
        instancesByAvailabilityZoneMultiMap.clear();
        for (PriamInstance ins : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
            instancesByAvailabilityZoneMultiMap.put(ins.getAvailabilityZone(), ins);
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
            if (instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).size() > 1 && instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(0).getHostIP().equals(myInstance.getHostIP())) {
                seeds.add(instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(1).getHostIP());
            }
        }
        logger.info("Retrieved seeds. My IP: {}, AZ-To-Instance-MultiMap: {}", myInstance.getHostIP(), instancesByAvailabilityZoneMultiMap);

        for (String loc : instancesByAvailabilityZoneMultiMap.keySet()) {
            seeds.add(instancesByAvailabilityZoneMultiMap.get(loc).get(0).getHostIP());
        }

        // Remove this node from the seed list so Cassandra auto-bootstrap will kick in.  Unless this is the only node in the cluster.
        if (seeds.size() > 1) {
            seeds.remove(myInstance.getHostIP());
        }

        return seeds;
    }

    public boolean isSeed() {
        populateInstancesByAvailabilityZoneMultiMap();
        String seedHostIPForAvailabilityZone = instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(0).getHostIP();
        return myInstance.getHostIP().equals(seedHostIPForAvailabilityZone);
    }

    public boolean isReplace() {
        return isReplace;
    }

    /**
     * Updates the Priam instance registry (SimpleDB) with the token currently in use by Cassandra.  Call this after
     * moving a server to a new token or else the move may be reverted if/when the server is replaced and the
     * replacement assigns the old token from SimpleDB.
     */
    public void updateToken() {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        myInstance.setToken(tokenManager.sanitizeToken(nodetool.getToken()));
        instanceRegistry.update(myInstance);
    }
}
