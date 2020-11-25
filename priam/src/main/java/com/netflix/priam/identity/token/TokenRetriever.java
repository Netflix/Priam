package com.netflix.priam.identity.token;

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
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenRetriever implements ITokenRetriever {

    public static final String NEW_SLOT = "new_slot";
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
    private boolean isTokenPregenerated = false;
    private String replacedIp;

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
            myInstance = grabExistingToken();
        }
        if (myInstance == null) {
            myInstance = grabNewToken();
        }
        logger.info("My instance: {}", myInstance);
        return myInstance;
    }

    @Override
    public Optional<String> getReplacedIp() {
        return Optional.ofNullable(replacedIp);
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
                        replacedIp = getReplacedIpForAssignedToken(liveNodes, instance.get());
                    }
                }
                return instance.orElse(null);
            }
        }.call();
    }

    @VisibleForTesting
    public PriamInstance grabExistingToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                logger.info("Trying to grab an existing token");
                sleeper.sleep(new Random().nextInt(5000) + 10000);
                Set<String> racInstanceIds = getRacInstanceIds();
                List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
                List<PriamInstance> instances =
                        allIds.stream()
                                .filter(i -> i.getRac().equals(myInstanceInfo.getRac()))
                                .filter(i -> !racInstanceIds.contains(i.getInstanceId()))
                                .collect(Collectors.toList());
                Optional<PriamInstance> candidate =
                        instances.stream().filter(i -> !isNew(i)).findFirst();
                candidate.ifPresent(i -> replacedIp = getReplacedIpForExistingToken(allIds, i));
                if (replacedIp == null) {
                    candidate = instances.stream().filter(i -> isNew(i)).findFirst();
                    candidate.ifPresent(i -> isTokenPregenerated = true);
                }
                return candidate.map(i -> claimToken(i)).orElse(null);
            }
        }.call();
    }

    private PriamInstance grabNewToken() throws Exception {
        Preconditions.checkState(config.isCreateNewTokenEnable());
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                set(100, 100);
                logger.info("Trying to generate a new token");
                sleeper.sleep(new Random().nextInt(15000));
                String myRegion = myInstanceInfo.getRegion();
                // this offset ensures the nodes are spread far away from the other regions.
                int regionOffset = tokenManager.regionOffset(myRegion);
                String myRac = myInstanceInfo.getRac();
                List<String> racs = config.getRacs();
                int mySlot =
                        factory.getAllIds(config.getAppName())
                                .stream()
                                .filter(i -> i.getRac().equals(myRac))
                                .map(PriamInstance::getId)
                                .max(Integer::compareTo)
                                .map(id -> racs.size() + Math.max(id, regionOffset) - regionOffset)
                                .orElseGet(
                                        () -> {
                                            Preconditions.checkState(racs.contains(myRac));
                                            return racs.indexOf(myRac);
                                        });
                int instanceCount = membership.getRacCount() * membership.getRacMembershipSize();
                String newToken = tokenManager.createToken(mySlot, instanceCount, myRegion);
                return createToken(mySlot + regionOffset, newToken);
            }
        }.call();
    }

    private String getReplacedIpForAssignedToken(
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
            if (!inferredIp.equals(myInstanceInfo.getHostIP())
                    && !inferredIp.equals(myInstanceInfo.getPrivateIP())) {
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
        return ipToReplace;
    }

    private String getReplacedIpForExistingToken(
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
                    return null;
                }
                // Everyone agreed to a value. Check if it is live node.
                if (inferredTokenInformation.getTokenInformation().isLive()) {
                    logger.info(
                            "This token is considered alive unanimously! We will not replace this instance.");
                    return null;
                } else {
                    String ip = inferredTokenInformation.getTokenInformation().getIpAddress();
                    logger.info("Will try to replace token owned by {}", ip);
                    return ip;
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
                return priamInstance.getHostIP();
            case MISMATCH:
                // Lets not replace the instance if gossip info is not merging!!
                logger.info(
                        "Mismatch in gossip. We will not replace this instance, until gossip settles down.");
                return null;
            default:
                throw new IllegalStateException(
                        "Unexpected value: "
                                + inferredTokenInformation.getTokenInformationStatus());
        }
    }

    private PriamInstance claimToken(PriamInstance originalInstance) {
        PriamInstance newInstance = new PriamInstance();
        newInstance.setApp(config.getAppName());
        newInstance.setId(originalInstance.getId());
        newInstance.setInstanceId(myInstanceInfo.getInstanceId());
        newInstance.setHost(myInstanceInfo.getHostname());
        newInstance.setHostIP(myInstanceInfo.getHostIP());
        newInstance.setRac(myInstanceInfo.getRac());
        newInstance.setVolumes(originalInstance.getVolumes());
        newInstance.setToken(originalInstance.getToken());
        newInstance.setDC(originalInstance.getDC());
        try {
            factory.update(originalInstance, newInstance);
        } catch (Exception ex) {
            long sleepTime = randomizer.nextInt(MAX_VALUE_IN_MILISECS);
            String token = newInstance.getToken();
            logger.warn("Failed updating token: {}; sleeping {} millis", token, sleepTime);
            sleeper.sleepQuietly(sleepTime);
            throw ex;
        }
        return newInstance;
    }

    private PriamInstance createToken(int id, String token) {
        try {
            return factory.create(
                    config.getAppName(),
                    id,
                    myInstanceInfo.getInstanceId(),
                    myInstanceInfo.getHostname(),
                    myInstanceInfo.getHostIP(),
                    myInstanceInfo.getRac(),
                    null /* volumes */,
                    token);
        } catch (Exception ex) {
            long sleepTime = randomizer.nextInt(MAX_VALUE_IN_MILISECS);
            logger.warn("Failed updating token: {}; sleeping {} millis", token, sleepTime);
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

    private boolean isNew(PriamInstance instance) {
        return instance.getInstanceId().equals(NEW_SLOT);
    }
}
