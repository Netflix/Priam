/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.priam.identity.token;

import com.google.common.collect.*;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.*;
import com.netflix.priam.identity.config.FakeInstanceInfo;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.lang3.math.Fraction;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/** Created by aagrawal on 3/1/19. */
public class TestNewTokenRetriever {
    @Mocked private IMembership membership;
    private IPriamInstanceFactory factory;
    private InstanceInfo instanceInfo;
    private IConfiguration configuration;

    private Map<String, String> tokenToEndpointMap =
            IntStream.range(0, 6)
                    .boxed()
                    .collect(
                            Collectors.toMap(String::valueOf, e -> String.format("127.0.0.%s", e)));
    private ImmutableList<String> liveInstances = ImmutableList.copyOf(tokenToEndpointMap.values());

    public TestNewTokenRetriever() {
        Injector injector = Guice.createInjector(new BRTestModule());
        instanceInfo = injector.getInstance(InstanceInfo.class);
        configuration = injector.getInstance(IConfiguration.class);
        factory = injector.getInstance(IPriamInstanceFactory.class);
    }

    @Test
    public void testDoubleTokenIntegrity() throws Exception {
        ((FakeConfiguration) configuration).setCreateNewToken(true);
        ((FakeConfiguration) configuration).setPartitioner("org.apache.cassandra.dht.RandomPartitioner");
        ImmutableSet<String> regions = ImmutableSet.of("us-east-1");
        ImmutableSetMultimap<String, String> racs = ImmutableSetMultimap.<String, String>builder()
            .put("us-east-1", "us-east-1c")
            .put("us-east-1", "us-east-1d")
            .put("us-east-1", "us-east-1e")
            .put("eu-west-1", "eu-west-1a")
            .put("eu-west-1", "eu-west-1b")
            .put("eu-west-1", "eu-west-1c")
            .put("us-west-2", "us-west-2a")
            .put("us-west-2", "us-west-2b")
            .put("us-west-2", "us-west-2c")
            .put("us-east-2", "us-east-2a")
            .put("us-east-2", "us-east-2b")
            .put("us-east-2", "us-east-2c")
            .build();
        int racSize = 4;
        new Expectations() {
            {
                membership.getRacMembershipSize();
                result = racSize;
            }
        };
        NewTokenRetriever tokenRetriever = getTokenRetriever();
        for (String region : regions) {
            ((FakeInstanceInfo) instanceInfo).setRegion(region);
            ImmutableSet<String> regionalRacs = racs.get(region);
            ((FakeConfiguration) configuration).setRacs(regionalRacs);
            new Expectations() {
                {
                    membership.getRacCount();
                    result = regionalRacs.size();
                }
            };
            for (String rac : regionalRacs) {
                ((FakeInstanceInfo) instanceInfo).setRac(rac);
                for (int instance = 0; instance < racSize; instance++) {
                    ListMultimap<String, PriamInstance> locMap = Multimaps.newListMultimap(new HashMap<>(), Lists::newArrayList);
                    List<PriamInstance> instances = factory.getAllIds(configuration.getAppName());
                    for (PriamInstance ins : instances) {
                        locMap.put(ins.getRac(), ins);
                    }
                    tokenRetriever.setLocMap(locMap);
                    tokenRetriever.get();
                }
            }
//            new DoubleRing(configuration, factory, new TokenManager(configuration), instanceInfo).doubleSlots();
//            new DoubleRing(configuration, factory, new TokenManager(configuration), instanceInfo).doubleSlots();
        }
        ((FakePriamInstanceFactory) factory).printInstances();
    }

    private String getStatus(List<String> liveInstances, Map<String, String> tokenToEndpointMap) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("live", liveInstances);
            jsonObject.put("tokenToEndpointMap", tokenToEndpointMap);
        } catch (Exception e) {

        }
        return jsonObject.toString();
    }

    private List<PriamInstance> getInstances(int noOfInstances) {
        List<PriamInstance> allInstances = Lists.newArrayList();
        for (int i = 1; i <= noOfInstances; i++) allInstances.add(createByIndex(i));
        return allInstances;
    }

    private PriamInstance createByIndex(int index) {
        return create(
                index,
                String.format("instance_id_%d", index),
                String.format("hostname_%d", index),
                String.format("127.0.0.%d", index),
                instanceInfo.getRac(),
                index + "");
    }

    private Set<String> getRacMembership(int noOfInstances) {
        return IntStream.range(1, noOfInstances + 1)
                .mapToObj(i -> String.format("instance_id_%d", i))
                .collect(Collectors.toSet());
    }

    private PriamInstance create(
            int id, String instanceID, String hostname, String ip, String rac, String payload) {
        return factory.create(
                configuration.getAppName(), id, instanceID, hostname, ip, rac, null, payload);
    }

    private NewTokenRetriever getTokenRetriever() {
        NewTokenRetriever tokenRetriever = new NewTokenRetriever(
            factory,
            membership,
            configuration,
            new FakeSleeper(),
            new TokenManager(configuration),
            instanceInfo);
        return tokenRetriever;
    }
}
