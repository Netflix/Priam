package com.netflix.priam.identity.token;

import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
        // Grab the token which was preassigned.
        logger.info("trying to grab preassigned token.");
        PriamInstance myInstance = grabPreAssignedToken();

        // Grab a dead token.
        if (myInstance == null) {
            logger.info("unable to grab preassigned token. trying to grab a dead token.");
            myInstance = grabDeadToken();
        }

        // Grab a pre-generated token if there is such one.
        if (myInstance == null) {
            logger.info("unable to grab a dead token. trying to grab a pregenerated token.");
            myInstance = grabPreGeneratedToken();
        }

        // Grab a new token
        if (myInstance == null) {
            logger.info("unable to grab a pregenerated token. trying to grab a new token.");
            myInstance = grabNewToken();
        }

        logger.info("My token: {}", myInstance.getToken());
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
                // Check if this node is decommissioned.
                List<PriamInstance> deadInstances =
                        factory.getAllIds(config.getAppName() + "-dead");
                PriamInstance instance = findInstance(deadInstances).orElse(null);
                if (instance != null) {
                    instance.setOutOfService(true);
                }

                if (instance == null) {
                    List<PriamInstance> aliveInstances = factory.getAllIds(config.getAppName());
                    instance = findInstance(aliveInstances).orElse(null);

                    if (instance != null) {
                        instance.setOutOfService(false);
                        // Priam might have crashed before bootstrapping Cassandra in replace mode.
                        // So, it is premature to use the assigned token without checking Cassandra
                        // gossip.
                        getReplacedIpForAssignedToken(aliveInstances, instance)
                                .ifPresent(ip -> setReplacedIp(ip));
                    }
                }

                if (instance != null) {
                    logger.info(
                            "Got token for {} node: {}",
                            instance.isOutOfService() ? "dead" : "live",
                            instance);
                }

                return instance;
            }
        }.call();
    }

    @VisibleForTesting
    public PriamInstance grabDeadToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {

                logger.info("Looking for a token from any dead node");
                final List<PriamInstance> allInstancesWithinCluster =
                        factory.getAllIds(config.getAppName());
                List<String> asgInstances = membership.getRacMembership();
                if (config.isDualAccount()) {
                    asgInstances = getDualAccountRacMembership(asgInstances);
                } else {
                    logger.info("Single Account cluster");
                }

                // Sleep random interval - upto 15 sec
                sleeper.sleep(new Random().nextInt(5000) + 10000);
                logger.info(
                        "About to iterate through all instances within cluster "
                                + config.getAppName()
                                + " to find an available token to acquire.");

                for (PriamInstance priamInstance : allInstancesWithinCluster) {
                    // test same zone and is it alive.
                    if (!priamInstance.getRac().equals(myInstanceInfo.getRac())
                            || asgInstances.contains(priamInstance.getInstanceId())
                            || isInstanceDummy(priamInstance)) continue;
                    // TODO: If instance is in SHUTTING_DOWN mode, it might not show up in asg
                    // instances (if cloud control plane is having issues), thus, we should not try
                    // to replace the instance as it will lead to "Cannot replace a live node"
                    // issue.

                    logger.info("Found dead instance: {}", priamInstance.toString());

                    PriamInstance markAsDead =
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

                    // find the replaced IP

                    // Infer current ownership information from other instances using gossip.
                    TokenRetrieverUtils.InferredTokenOwnership inferredTokenInformation =
                            TokenRetrieverUtils.inferTokenOwnerFromGossip(
                                    allInstancesWithinCluster,
                                    priamInstance.getToken(),
                                    priamInstance.getDC());

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
                            } else
                                replacedIp =
                                        inferredTokenInformation
                                                .getTokenInformation()
                                                .getIpAddress();
                            break;
                        case UNREACHABLE_NODES:
                            // In case of unable to reach sufficient nodes, fallback to IP in token
                            // database. This could be a genuine case of say missing security
                            // permissions.
                            replacedIp = priamInstance.getHostIP();
                            logger.warn(
                                    "Unable to reach sufficient nodes. Please check security group permissions or there might be a network partition.");
                            logger.info(
                                    "Will try to replace token: {} with replacedIp from Token database: {}",
                                    priamInstance.getToken(),
                                    priamInstance.getHostIP());
                            break;
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

                    PriamInstance result;
                    try {
                        result =
                                factory.create(
                                        config.getAppName(),
                                        markAsDead.getId(),
                                        myInstanceInfo.getInstanceId(),
                                        myInstanceInfo.getHostname(),
                                        myInstanceInfo.getHostIP(),
                                        myInstanceInfo.getRac(),
                                        markAsDead.getVolumes(),
                                        markAsDead.getToken());
                    } catch (Exception ex) {
                        long sleepTime = getSleepTime();
                        logger.warn(
                                "Exception when acquiring dead token: "
                                        + priamInstance.getToken()
                                        + " , will sleep for "
                                        + sleepTime
                                        + " millisecs before we retry.");
                        Thread.sleep(sleepTime);

                        throw ex;
                    }

                    logger.info(
                            "Acquired token: "
                                    + priamInstance.getToken()
                                    + " and we will replace with replacedIp: "
                                    + replacedIp);

                    if (result != null) {
                        isReplace = true;
                    }
                    return result;
                }

                logger.info("This node was NOT able to acquire any dead token");
                return null;
            }
        }.call();
    }

    private PriamInstance grabPreGeneratedToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                logger.info("Looking for any pre-generated token");

                final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
                List<String> asgInstances = membership.getRacMembership();
                // Sleep random interval - upto 15 sec
                sleeper.sleep(new Random().nextInt(5000) + 10000);
                PriamInstance result = null;
                for (PriamInstance dead : allIds) {
                    // test same zone and is it is alive.
                    if (!dead.getRac().equals(myInstanceInfo.getRac())
                            || asgInstances.contains(dead.getInstanceId())
                            || !isInstanceDummy(dead)) continue;
                    logger.info("Found pre-generated token: {}", dead.getToken());
                    PriamInstance markAsDead =
                            factory.create(
                                    dead.getApp() + "-dead",
                                    dead.getId(),
                                    dead.getInstanceId(),
                                    dead.getHostName(),
                                    dead.getHostIP(),
                                    dead.getRac(),
                                    dead.getVolumes(),
                                    dead.getToken());
                    // remove it as we marked it down...
                    factory.delete(dead);

                    String payLoad = markAsDead.getToken();
                    logger.info(
                            "Trying to grab slot {} with availability zone {}",
                            markAsDead.getId(),
                            markAsDead.getRac());
                    result =
                            factory.create(
                                    config.getAppName(),
                                    markAsDead.getId(),
                                    myInstanceInfo.getInstanceId(),
                                    myInstanceInfo.getHostname(),
                                    myInstanceInfo.getHostIP(),
                                    myInstanceInfo.getRac(),
                                    markAsDead.getVolumes(),
                                    payLoad);
                    break;
                }
                if (result != null) {
                    isTokenPregenerated = true;
                }
                return result;
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
                logger.info("Generating my own and new token");
                // Sleep random interval - upto 15 sec
                sleeper.sleep(new Random().nextInt(15000));
                int hash = tokenManager.regionOffset(myInstanceInfo.getRegion());
                // use this hash so that the nodes are spread far away from the other
                // regions.

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

    private Optional<PriamInstance> findInstance(List<PriamInstance> instances) {
        return instances
                .stream()
                .filter((i) -> i.getInstanceId().equals(myInstanceInfo.getInstanceId()))
                .findFirst();
    }

    public void setReplacedIp(String replacedIp) {
        this.replacedIp = replacedIp;
        if (!replacedIp.isEmpty()) this.isReplace = true;
    }

    private List<String> getDualAccountRacMembership(List<String> asgInstances) {
        logger.info("Dual Account cluster");

        List<String> crossAccountAsgInstances = membership.getCrossAccountRacMembership();

        // Remove duplicates (probably there are not)
        asgInstances.removeAll(crossAccountAsgInstances);

        // Merge the two lists
        asgInstances.addAll(crossAccountAsgInstances);
        logger.info("Combined Instances in the AZ: {}", asgInstances);

        return asgInstances;
    }

    private long getSleepTime() {
        return this.randomizer.nextInt(MAX_VALUE_IN_MILISECS);
    }

    private boolean isInstanceDummy(PriamInstance instance) {
        return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
}
