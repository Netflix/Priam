/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.identity;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.identity.token.IDeadTokenRetriever;
import com.netflix.priam.identity.token.INewTokenRetriever;
import com.netflix.priam.identity.token.IPreGeneratedTokenRetriever;
import com.netflix.priam.identity.token.TokenRetrieverUtils;
import com.netflix.priam.identity.token.TokenRetrieverUtils.GossipParseException;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the central place to create and consume the identity of the instance - token,
 * seeds etc.
 */
@Singleton
public class InstanceIdentity {
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);
    public static final String DUMMY_INSTANCE_ID = "new_slot";

    private final ListMultimap<String, PriamInstance> locMap =
            Multimaps.newListMultimap(new HashMap<>(), Lists::newArrayList);
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private final ITokenManager tokenManager;

    private final Predicate<PriamInstance> differentHostPredicate =
            new Predicate<PriamInstance>() {
                @Override
                public boolean apply(PriamInstance instance) {
                    return (!instance.getInstanceId().equalsIgnoreCase(DUMMY_INSTANCE_ID)
                            && !instance.getHostName().equals(myInstance.getHostName()));
                }
            };

    private PriamInstance myInstance;
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
    // Note: do not parameterized the generic type variable to an implementation as
    // it confuses
    // Guice in the binding.
    public InstanceIdentity(
            IPriamInstanceFactory factory,
            IMembership membership,
            IConfiguration config,
            Sleeper sleeper,
            ITokenManager tokenManager,
            IDeadTokenRetriever deadTokenRetriever,
            IPreGeneratedTokenRetriever preGeneratedTokenRetriever,
            INewTokenRetriever newTokenRetriever,
            InstanceInfo instanceInfo)
            throws Exception {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.sleeper = sleeper;
        this.tokenManager = tokenManager;
        this.deadTokenRetriever = deadTokenRetriever;
        this.preGeneratedTokenRetriever = preGeneratedTokenRetriever;
        this.newTokenRetriever = newTokenRetriever;
        this.myInstanceInfo = instanceInfo;
        init();
    }

    public PriamInstance getInstance() {
        return myInstance;
    }

    public InstanceInfo getInstanceInfo() {
        return myInstanceInfo;
    }

    public void init() throws Exception {
        // Grab the token which was preassigned.
        logger.info("trying to grab preassigned token.");
        myInstance = grabPreAssignedToken();

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
                        try {
                            String replaceIp =
                                    TokenRetrieverUtils.inferTokenOwnerFromGossip(
                                            aliveInstances, instance.getToken(), instance.getDC());
                            if (!StringUtils.isEmpty(replaceIp)
                                    && !replaceIp.equals(instance.getHostIP())) {
                                setReplacedIp(replaceIp);
                                logger.info(
                                        "Priam found that the token is not alive according to Cassandra and we should start Cassandra in replace mode with replace ip: "
                                                + replaceIp);
                            }
                        } catch (GossipParseException e) {
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
                    isReplace = true; // indicate that we are acquiring a dead instance's token.

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

    public List<String> getSeeds() throws UnknownHostException {
        populateRacMap();
        List<String> seeds = new LinkedList<>();
        // Handle single zone deployment
        if (config.getRacs().size() == 1) {
            // Return empty list if all nodes are not up
            if (membership.getRacMembershipSize() != locMap.get(myInstance.getRac()).size())
                return seeds;
            // If seed node, return the next node in the list
            if (locMap.get(myInstance.getRac()).size() > 1
                    && locMap.get(myInstance.getRac())
                            .get(0)
                            .getHostIP()
                            .equals(myInstance.getHostIP())) {
                PriamInstance instance = locMap.get(myInstance.getRac()).get(1);
                if (instance != null && !isInstanceDummy(instance)) {
                    if (config.isMultiDC()) seeds.add(instance.getHostIP());
                    else seeds.add(instance.getHostName());
                }
            }
        }
        for (String loc : locMap.keySet()) {
            PriamInstance instance =
                    Iterables.tryFind(locMap.get(loc), differentHostPredicate).orNull();
            if (instance != null && !isInstanceDummy(instance)) {
                if (config.isMultiDC()) seeds.add(instance.getHostIP());
                else seeds.add(instance.getHostName());
            }
        }
        return seeds;
    }

    public boolean isSeed() {
        populateRacMap();
        String ip = locMap.get(myInstance.getRac()).get(0).getHostName();
        return myInstance.getHostName().equals(ip);
    }

    public boolean isReplace() {
        return isReplace;
    }

    public boolean isTokenPregenerated() {
        return isTokenPregenerated;
    }

    public String getReplacedIp() {
        return replacedIp;
    }

    public void setReplacedIp(String replacedIp) {
        this.replacedIp = replacedIp;
        if (!replacedIp.isEmpty()) this.isReplace = true;
        else this.isReplace = false;
    }

    private static boolean isInstanceDummy(PriamInstance instance) {
        return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
}
