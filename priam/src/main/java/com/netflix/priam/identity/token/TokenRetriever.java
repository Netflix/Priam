package com.netflix.priam.identity.token;

import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenRetriever implements ITokenRetriever {

    public static final String DUMMY_INSTANCE_ID = "new_slot";
    private static final int MAX_VALUE_IN_MILISECS = 300000; // sleep up to 5 minutes
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);

    private final Random randomizer;
    private final Sleeper sleeper;
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final ITokenManager tokenManager;

    // Instance information contains other information like ASG/vpc-id etc.
    private InstanceInfo myInstanceInfo;
    private boolean isReplace = false;
    private boolean isTokenPregenerated = false;
    private String replacedIp = "";

    @Inject
    public TokenRetriever(
            IPriamInstanceFactory factory,
            IMembership membership,
            IConfiguration config,
            InstanceInfo instanceInfo,
            Sleeper sleeper,
            ITokenManager tokenManager) {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.myInstanceInfo = instanceInfo;
        this.randomizer = new Random();
        this.sleeper = sleeper;
        this.tokenManager = tokenManager;
    }

    @Override
    public PriamInstance get() throws Exception {
        PriamInstance myInstance = grabPreAssignedToken();
        if (myInstance == null) {
            myInstance = grabDeadToken();
        }
        if (myInstance == null) {
            myInstance = grabPreGeneratedToken();
        }
        if (myInstance == null) {
            myInstance = grabNewToken();
        }
        logger.info("My instance: {}", myInstance);
        return myInstance;
    }

    @Override
    public String getReplacedIp() {
        return replacedIp;
    }

    @Override
    public boolean isReplace() {
        return isReplace;
    }

    @Override
    public boolean isTokenPregenerated() {
        return isTokenPregenerated;
    }

    private PriamInstance grabPreAssignedToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                logger.info("Trying to grab a pre-assigned token.");
                // Check if this node is decommissioned.
                List<PriamInstance> allIds = factory.getAllIds(config.getAppName() + "-dead");
                Optional<PriamInstance> instance =
                        findInstance(allIds).map(PriamInstance::setOutOfService);
                if (!instance.isPresent()) {
                    List<PriamInstance> liveNodes = factory.getAllIds(config.getAppName());
                    instance = instance.map(Optional::of).orElseGet(() -> findInstance(liveNodes));
                    if (instance.isPresent()) {
                        // Why check gossip? Priam might have crashed before bootstrapping
                        // Cassandra in replace mode.
                        Optional<String> optionalIp =
                                getReplacedIpForAssignedToken(liveNodes, instance.get());
                        optionalIp.ifPresent(ip -> replacedIp = ip);
                        optionalIp.ifPresent(ip -> isReplace = true);
                    }
                }
                return instance.orElse(null);
            }
        }.call();
    }

    @VisibleForTesting
    public PriamInstance grabDeadToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                logger.info("Trying to grab a dead token");
                sleeper.sleep(new Random().nextInt(5000) + 10000);
                final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
                Set<String> asgInstances = getRacInstanceIds();
                for (PriamInstance instance : allIds) {
                    if (!instance.getRac().equals(myInstanceInfo.getRac())
                            || asgInstances.contains(instance.getInstanceId())
                            || isInstanceDummy(instance)) continue;
                    markDead(instance);
                    Optional<String> ipToReplace = getReplacedIpForExistingToken(allIds, instance);
                    if (ipToReplace.isPresent()) {
                        replacedIp = ipToReplace.get();
                        isReplace = true;
                        return claimToken(instance);
                    }
                }
                return null;
            }
        }.call();
    }

    private PriamInstance grabPreGeneratedToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                logger.info("Trying to grab a pre-generated token");
                sleeper.sleep(new Random().nextInt(5000) + 10000);
                final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
                Set<String> asgInstances = getRacInstanceIds();
                for (PriamInstance dead : allIds) {
                    if (!dead.getRac().equals(myInstanceInfo.getRac())
                            || asgInstances.contains(dead.getInstanceId())
                            || !isInstanceDummy(dead)) continue;
                    markDead(dead);
                    isTokenPregenerated = true;
                    return claimToken(dead);
                }
                return null;
            }
        }.call();
    }

    private PriamInstance grabNewToken() throws Exception {
        if (!this.config.isCreateNewTokenEnable()) {
            throw new IllegalStateException(
                    "Node attempted to erroneously create a new token when we should be grabbing an existing token.");
        }

        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                set(100, 100);
                logger.info("Trying to generate a new token");
                sleeper.sleep(new Random().nextInt(15000));
                // this hash ensures the nodes are spread far away from the other regions.
                int hash = tokenManager.regionOffset(myInstanceInfo.getRegion());
                List<Integer> racIds =
                        factory.getAllIds(config.getAppName())
                                .stream()
                                .filter(i -> i.getRac().equals(myInstanceInfo.getRac()))
                                .map(PriamInstance::getId)
                                .collect(toList());
                int max = Math.max(hash, racIds.stream().max(Integer::compareTo).orElse(hash));
                int maxSlot = max - hash;
                int my_slot;
                if (hash == max && racIds.isEmpty()) {
                    int idx = config.getRacs().indexOf(myInstanceInfo.getRac());
                    if (idx < 0)
                        throw new Exception(
                                String.format(
                                        "Rac %s is not in Racs %s",
                                        myInstanceInfo.getRac(), config.getRacs()));
                    my_slot = idx + maxSlot;
                } else my_slot = config.getRacs().size() + maxSlot;
                logger.info(
                        "Trying to createToken with slot {} with rac count {} with rac membership size {} with dc {}",
                        my_slot,
                        membership.getRacCount(),
                        membership.getRacMembershipSize(),
                        myInstanceInfo.getRegion());
                String payload =
                        tokenManager.createToken(
                                my_slot,
                                membership.getRacCount(),
                                membership.getRacMembershipSize(),
                                myInstanceInfo.getRegion());
                return factory.create(
                        config.getAppName(),
                        my_slot + hash,
                        myInstanceInfo.getInstanceId(),
                        myInstanceInfo.getHostname(),
                        myInstanceInfo.getHostIP(),
                        myInstanceInfo.getRac(),
                        null,
                        payload);
            }
        }.call();
    }

    private Optional<String> getReplacedIpForAssignedToken(
            List<PriamInstance> aliveInstances, PriamInstance instance)
            throws TokenRetrieverUtils.GossipParseException {
        // Infer current ownership information from other instances using gossip.
        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        aliveInstances, instance.getToken(), instance.getDC());
        // if unreachable rely on token database.
        // if mismatch rely on token database.
        String ipToReplace = null;
        if (inferredTokenOwnership.getTokenInformationStatus()
                == TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.GOOD) {
            Preconditions.checkNotNull(inferredTokenOwnership.getTokenInformation());
            String inferredIp = inferredTokenOwnership.getTokenInformation().getIpAddress();
            if (!inferredIp.equalsIgnoreCase(instance.getHostIP())) {
                if (inferredTokenOwnership.getTokenInformation().isLive()) {
                    throw new TokenRetrieverUtils.GossipParseException(
                            "We have been assigned a token that C* thinks is alive. Throwing to buy time in the hopes that Gossip just needs to settle.");
                }
                ipToReplace = inferredIp;
                logger.info(
                        "Priam found that the token is not alive according to Cassandra and we should start Cassandra in replace mode with replace ip: "
                                + inferredIp);
            }
        }
        return Optional.ofNullable(ipToReplace);
    }

    private Optional<String> getReplacedIpForExistingToken(
            List<PriamInstance> allInstancesWithinCluster, PriamInstance priamInstance) {

        // Infer current ownership information from other instances using gossip.
        TokenRetrieverUtils.InferredTokenOwnership inferredTokenInformation =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        allInstancesWithinCluster, priamInstance.getToken(), priamInstance.getDC());

        switch (inferredTokenInformation.getTokenInformationStatus()) {
            case GOOD:
                if (inferredTokenInformation.getTokenInformation() == null) {
                    logger.error(
                            "If you see this message, it should not have happened. We expect token ownership information if all nodes agree. This is a code bounty issue.");
                    return Optional.empty();
                }
                // Everyone agreed to a value. Check if it is live node.
                if (inferredTokenInformation.getTokenInformation().isLive()) {
                    logger.info(
                            "This token is considered alive unanimously! We will not replace this instance.");
                    return Optional.empty();
                } else {
                    String ip = inferredTokenInformation.getTokenInformation().getIpAddress();
                    logger.info("Will try to replace token owned by {}", ip);
                    return Optional.of(ip);
                }
            case UNREACHABLE_NODES:
                // In case of unable to reach sufficient nodes, fallback to IP in token
                // database. This could be a genuine case of say missing security
                // permissions.
                logger.warn(
                        "Unable to reach sufficient nodes. Please check security group permissions or there might be a network partition.");
                logger.info(
                        "Will try to replace token: {} with replacedIp from Token database: {}",
                        priamInstance.getToken(),
                        priamInstance.getHostIP());
                return Optional.of(priamInstance.getHostIP());
            case MISMATCH:
                // Lets not replace the instance if gossip info is not merging!!
                logger.info(
                        "Mismatch in gossip. We will not replace this instance, until gossip settles down.");
                return Optional.empty();
            default:
                throw new IllegalStateException(
                        "Unexpected value: "
                                + inferredTokenInformation.getTokenInformationStatus());
        }
    }

    private void markDead(PriamInstance priamInstance) {
        factory.create(
                priamInstance.getApp() + "-dead",
                priamInstance.getId(),
                priamInstance.getInstanceId(),
                priamInstance.getHostName(),
                priamInstance.getHostIP(),
                priamInstance.getRac(),
                priamInstance.getVolumes(),
                priamInstance.getToken());
        // remove it as we marked it down...
        factory.delete(priamInstance);
    }

    private PriamInstance claimToken(PriamInstance instance) {
        try {
            return factory.create(
                    config.getAppName(),
                    instance.getId(),
                    myInstanceInfo.getInstanceId(),
                    myInstanceInfo.getHostname(),
                    myInstanceInfo.getHostIP(),
                    myInstanceInfo.getRac(),
                    instance.getVolumes(),
                    instance.getToken());
        } catch (Exception ex) {
            long sleepTime = randomizer.nextInt(MAX_VALUE_IN_MILISECS);
            logger.warn(
                    "Failed creating token: {}; sleeping {} millis",
                    instance.getToken(),
                    sleepTime);
            sleeper.sleepQuietly(sleepTime);
            throw ex;
        }
    }

    private Optional<PriamInstance> findInstance(List<PriamInstance> instances) {
        return instances
                .stream()
                .filter((i) -> i.getInstanceId().equals(myInstanceInfo.getInstanceId()))
                .findFirst();
    }

    private Set<String> getRacInstanceIds() { // TODO(CASS-1986)
        ImmutableSet<String> racMembership = membership.getRacMembership();
        return config.isDualAccount()
                ? Sets.union(membership.getCrossAccountRacMembership(), racMembership)
                : racMembership;
    }

    private boolean isInstanceDummy(PriamInstance instance) {
        return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
}
