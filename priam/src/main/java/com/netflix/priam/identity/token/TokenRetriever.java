package com.netflix.priam.identity.token;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import java.util.*;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenRetriever implements ITokenRetriever {

    public static final String DUMMY_INSTANCE_ID = "new_slot";
    private static final int MAX_VALUE_IN_MILISECS = 300000; // sleep up to 5 minutes
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);

    private final Random randomizer;
    private final Sleeper sleeper;
    private final ListMultimap<String, PriamInstance> locMap =
            Multimaps.newListMultimap(new HashMap<>(), Lists::newArrayList);
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final IMembership membership;
    private final IConfiguration config;

    // Instance information contains other information like ASG/vpc-id etc.
    private InstanceInfo myInstanceInfo;
    private boolean isReplace = false;
    private boolean isTokenPregenerated = false;
    private String replacedIp = "";
    private final IPreGeneratedTokenRetriever preGeneratedTokenRetriever;
    private final INewTokenRetriever newTokenRetriever;

    private final java.util.function.Predicate<PriamInstance> sameHostPredicate =
            (i) -> i.getInstanceId().equals(myInstanceInfo.getInstanceId());

    @Inject
    public TokenRetriever(
            IPriamInstanceFactory factory,
            IMembership membership,
            IConfiguration config,
            IPreGeneratedTokenRetriever preGeneratedTokenRetriever,
            INewTokenRetriever newTokenRetriever,
            InstanceInfo instanceInfo,
            Sleeper sleeper) {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.preGeneratedTokenRetriever = preGeneratedTokenRetriever;
        this.newTokenRetriever = newTokenRetriever;
        this.myInstanceInfo = instanceInfo;
        this.randomizer = new Random();
        this.sleeper = sleeper;
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
                PriamInstance instance =
                        findInstance(deadInstances, sameHostPredicate).orElse(null);
                if (instance != null) {
                    instance.setOutOfService(true);
                }

                if (instance == null) {
                    List<PriamInstance> aliveInstances = factory.getAllIds(config.getAppName());
                    instance = findInstance(aliveInstances, sameHostPredicate).orElse(null);

                    if (instance != null) {
                        instance.setOutOfService(false);

                        // Priam might have crashed before bootstrapping Cassandra in replace mode.
                        // So, it is premature to use the assigned token without checking Cassandra
                        // gossip.

                        // Infer current ownership information from other instances using gossip.
                        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                                        aliveInstances, instance.getToken(), instance.getDC());
                        // if unreachable rely on token database.
                        // if mismatch rely on token database.
                        if (inferredTokenOwnership.getTokenInformationStatus()
                                == TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus
                                        .GOOD) {
                            Preconditions.checkNotNull(
                                    inferredTokenOwnership.getTokenInformation());
                            String inferredIp =
                                    inferredTokenOwnership.getTokenInformation().getIpAddress();
                            if (!inferredIp.equalsIgnoreCase(instance.getHostIP())) {
                                if (inferredTokenOwnership.getTokenInformation().isLive()) {
                                    throw new TokenRetrieverUtils.GossipParseException(
                                            "We have been assigned a token that C* thinks is alive. Throwing to buy time in the hopes that Gossip just needs to settle.");
                                }
                                setReplacedIp(inferredIp);
                                logger.info(
                                        "Priam found that the token is not alive according to Cassandra and we should start Cassandra in replace mode with replace ip: "
                                                + inferredIp);
                            }
                        }
                    }
                }

                if (instance != null) {
                    logger.info(
                            "{} found that this node is {}."
                                    + " application: {},"
                                    + " id: {},"
                                    + " instance: {},"
                                    + " region: {},"
                                    + " host ip: {},"
                                    + " host name: {},"
                                    + " token: {}",
                            instance.isOutOfService() ? "[Dead]" : "[Alive]",
                            instance.isOutOfService() ? "dead" : "alive",
                            instance.getApp(),
                            instance.getId(),
                            instance.getInstanceId(),
                            instance.getDC(),
                            instance.getHostIP(),
                            instance.getHostName(),
                            instance.getToken());
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
                PriamInstance result = preGeneratedTokenRetriever.get();
                if (result != null) {
                    isTokenPregenerated = true;
                }
                return result;
            }

            @Override
            public void forEachExecution() {
                populateRacMap();
                preGeneratedTokenRetriever.setLocMap(locMap);
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
                newTokenRetriever.setLocMap(locMap);
                return newTokenRetriever.get();
            }

            @Override
            public void forEachExecution() {
                populateRacMap();
                newTokenRetriever.setLocMap(locMap);
            }
        }.call();
    }

    private Optional<PriamInstance> findInstance(
            List<PriamInstance> instances, java.util.function.Predicate<PriamInstance> predicate) {
        return Optional.ofNullable(instances)
                .orElse(Collections.emptyList())
                .stream()
                .filter(predicate)
                .findFirst();
    }

    private void populateRacMap() {
        locMap.clear();
        List<PriamInstance> instances = factory.getAllIds(config.getAppName());
        for (PriamInstance ins : instances) {
            locMap.put(ins.getRac(), ins);
        }
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
