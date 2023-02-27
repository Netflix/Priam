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
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.identity.token.ITokenRetriever;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This class provides the central place to create and consume the identity of the instance - token,
 * seeds etc.
 */
@Singleton
public class InstanceIdentity {
    public static final String DUMMY_INSTANCE_ID = "new_slot";

    private final ListMultimap<String, PriamInstance> locMap =
            Multimaps.newListMultimap(new HashMap<>(), Lists::newArrayList);
    private final IPriamInstanceFactory factory;
    private final IMembership membership;
    private final IConfiguration config;

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
    private boolean isReplace;
    private boolean isTokenPregenerated;
    private String replacedIp;

    @Inject
    // Note: do not parameterized the generic type variable to an implementation as
    // it confuses
    // Guice in the binding.
    public InstanceIdentity(
            IPriamInstanceFactory factory,
            IMembership membership,
            IConfiguration config,
            InstanceInfo instanceInfo,
            ITokenRetriever tokenRetriever)
            throws Exception {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.myInstanceInfo = instanceInfo;
        this.myInstance = tokenRetriever.get();
        this.replacedIp = tokenRetriever.getReplacedIp().orElse(null);
        this.isReplace = replacedIp != null;
        this.isTokenPregenerated = tokenRetriever.isTokenPregenerated();
    }

    public PriamInstance getInstance() {
        return myInstance;
    }

    public InstanceInfo getInstanceInfo() {
        return myInstanceInfo;
    }

    private void populateRacMap() {
        locMap.clear();
        factory.getAllIds(config.getAppName()).forEach(ins -> locMap.put(ins.getRac(), ins));
    }

    public List<String> getSeeds() {
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
    }

    private static boolean isInstanceDummy(PriamInstance instance) {
        return instance.getInstanceId().equals(DUMMY_INSTANCE_ID);
    }
}
