package com.netflix.priam.identity.token;

import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.RetryableCallable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenRetriever implements ITokenRetriever {

    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);

    private final ListMultimap<String, PriamInstance> locMap =
            Multimaps.newListMultimap(new HashMap<>(), Lists::newArrayList);
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final IConfiguration config;

    // Instance information contains other information like ASG/vpc-id etc.
    private InstanceInfo myInstanceInfo;
    private boolean isReplace = false;
    private boolean isTokenPregenerated = false;
    private String replacedIp = "";
    private final IDeadTokenRetriever deadTokenRetriever;
    private final IPreGeneratedTokenRetriever preGeneratedTokenRetriever;
    private final INewTokenRetriever newTokenRetriever;

    private final java.util.function.Predicate<PriamInstance> sameHostPredicate =
            (i) -> i.getInstanceId().equals(myInstanceInfo.getInstanceId());

    @Inject
    public TokenRetriever(
            IPriamInstanceFactory factory,
            IConfiguration config,
            IDeadTokenRetriever deadTokenRetriever,
            IPreGeneratedTokenRetriever preGeneratedTokenRetriever,
            INewTokenRetriever newTokenRetriever,
            InstanceInfo instanceInfo) {
        this.factory = factory;
        this.config = config;
        this.deadTokenRetriever = deadTokenRetriever;
        this.preGeneratedTokenRetriever = preGeneratedTokenRetriever;
        this.newTokenRetriever = newTokenRetriever;
        this.myInstanceInfo = instanceInfo;
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

    private PriamInstance grabDeadToken() throws Exception {
        return new RetryableCallable<PriamInstance>() {
            @Override
            public PriamInstance retriableCall() throws Exception {
                PriamInstance result = deadTokenRetriever.get();
                if (result != null) {
                    isReplace = true;

                    if (deadTokenRetriever.getReplaceIp()
                            != null) { // The IP address of the dead instance to which
                        // we will acquire its token
                        replacedIp = deadTokenRetriever.getReplaceIp();
                    }
                }

                return result;
            }

            @Override
            public void forEachExecution() {
                populateRacMap();
                deadTokenRetriever.setLocMap(locMap);
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
}
