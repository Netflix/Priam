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
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.FakeInstanceInfo;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.SystemUtils;
import com.netflix.priam.utils.TokenManager;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.lang3.math.Fraction;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/** Created by aagrawal on 3/1/19. */
public class TokenRetrieverTest {
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

    public TokenRetrieverTest() {
        Injector injector = Guice.createInjector(new BRTestModule());
        instanceInfo = injector.getInstance(InstanceInfo.class);
        configuration = injector.getInstance(IConfiguration.class);
        factory = injector.getInstance(IPriamInstanceFactory.class);
    }

    @Test
    public void testNoReplacementNormalScenario() throws Exception {
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of();
            }
        };
        PriamInstance priamInstance = getTokenRetriever().grabExistingToken();
        Truth.assertThat(priamInstance).isNull();
    }

    @Test
    // There is no slot available for replacement as per Token Database.
    public void testNoReplacementNoSpotAvailable() throws Exception {
        List<PriamInstance> allInstances = getInstances(1);
        Set<String> racMembership = getRacMembership(1);
        racMembership.add(instanceInfo.getInstanceId());
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.copyOf(racMembership);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        Truth.assertThat(tokenRetriever.grabExistingToken()).isNull();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isFalse();
        Truth.assertThat(factory.getAllIds(configuration.getAppName()))
                .containsExactlyElementsIn(allInstances);
    }

    @Test
    // There is a potential slot for dead token but we are unable to replace.
    public void testNoReplacementNoGossipMatch(@Mocked SystemUtils systemUtils) throws Exception {
        getInstances(2);
        Set<String> racMembership = getRacMembership(1);
        racMembership.add(instanceInfo.getInstanceId());
        // gossip info returns null, thus unable to replace the instance.
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.copyOf(racMembership);
                SystemUtils.getDataFromUrl(anyString);
                result = getStatus(liveInstances, tokenToEndpointMap);
                times = 1;
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        Truth.assertThat(tokenRetriever.grabExistingToken()).isNull();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isFalse();
    }

    @Test
    // There is a potential slot for dead token but we are unable to replace.
    public void testUsePregeneratedTokenWhenThereIsNoGossipMatchForDeadToken(
            @Mocked SystemUtils systemUtils) throws Exception {
        create(0, "iid_0", "host_0", "127.0.0.0", instanceInfo.getRac(), 0 + "");
        create(1, "new_slot", "host_1", "127.0.0.1", instanceInfo.getRac(), 1 + "");
        // gossip info returns null, thus unable to replace the instance.
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of();
                SystemUtils.getDataFromUrl(anyString);
                result = getStatus(liveInstances, tokenToEndpointMap);
                times = 1;
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        PriamInstance instance = tokenRetriever.grabExistingToken();
        Truth.assertThat(instance).isNotNull();
        Truth.assertThat(instance.getId()).isEqualTo(1);
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isFalse();
    }

    @Test
    public void testReplacementGossipMatch(@Mocked SystemUtils systemUtils) throws Exception {
        getInstances(6);
        Set<String> racMembership = getRacMembership(2);
        racMembership.add(instanceInfo.getInstanceId());

        List<String> myliveInstances =
                liveInstances
                        .stream()
                        .filter(x -> !x.equalsIgnoreCase("127.0.0.3"))
                        .collect(Collectors.toList());
        String gossipResponse = getStatus(myliveInstances, tokenToEndpointMap);

        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.copyOf(racMembership);
                SystemUtils.getDataFromUrl(anyString);
                returns(gossipResponse, gossipResponse, null, "random_value", gossipResponse);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        Truth.assertThat(tokenRetriever.grabExistingToken()).isNotNull();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isTrue();
        Truth.assertThat(tokenRetriever.getReplacedIp().get()).isEqualTo("127.0.0.3");
    }

    @Test
    public void testPrioritizeDeadTokens(@Mocked SystemUtils systemUtils) throws Exception {
        create(0, "iid_0", "host_0", "127.0.0.0", instanceInfo.getRac(), 0 + "");
        create(1, "new_slot", "host_1", "127.0.0.1", instanceInfo.getRac(), 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of();
                SystemUtils.getDataFromUrl(anyString);
                returns(null, null);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        Truth.assertThat(tokenRetriever.grabExistingToken()).isNotNull();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isTrue();
        Truth.assertThat(tokenRetriever.getReplacedIp().get()).isEqualTo("127.0.0.0");
    }

    @Test
    public void testPrioritizeDeadInstancesEvenIfAfterANewSlot(@Mocked SystemUtils systemUtils)
            throws Exception {
        create(0, "new_slot", "host_0", "127.0.0.0", instanceInfo.getRac(), 0 + "");
        create(1, "iid_1", "host_1", "127.0.0.1", instanceInfo.getRac(), 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of();
                SystemUtils.getDataFromUrl(anyString);
                returns(null, null);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        Truth.assertThat(tokenRetriever.grabExistingToken()).isNotNull();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isTrue();
        Truth.assertThat(tokenRetriever.getReplacedIp().get()).isEqualTo("127.0.0.1");
    }

    @Test
    public void testNewTokenFailureIfProhibited() {
        ((FakeConfiguration) configuration).setCreateNewToken(false);
        create(0, "iid_0", "host_0", "127.0.0.0", instanceInfo.getRac(), 0 + "");
        create(1, "iid_1", "host_1", "127.0.0.1", instanceInfo.getRac(), 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of("iid_0", "iid_1");
            }
        };
        Assertions.assertThrows(IllegalStateException.class, () -> getTokenRetriever().get());
    }

    @Test
    public void testNewTokenNoInstancesInRac() throws Exception {
        create(0, "iid_0", "host_0", "127.0.0.0", "az2", 0 + "");
        create(1, "iid_1", "host_1", "127.0.0.1", "az2", 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of("iid_0", "iid_1");
                membership.getRacCount();
                result = 1;
                membership.getRacMembershipSize();
                result = 3;
            }
        };
        PriamInstance instance = getTokenRetriever().get();
        Truth.assertThat(instance.getToken()).isEqualTo("1808575600");
        // region offset for us-east-1 + index of rac az1 (1808575600 + 0)
        Truth.assertThat(instance.getId()).isEqualTo(1808575600);
    }

    @Test
    public void testNewTokenGenerationNoInstancesWithLargeEnoughId() throws Exception {
        create(0, "iid_0", "host_0", "127.0.0.0", "az1", 0 + "");
        create(1, "iid_1", "host_1", "127.0.0.1", "az1", 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of("iid_0", "iid_1");
                membership.getRacCount();
                result = 1;
                membership.getRacMembershipSize();
                result = 3;
            }
        };
        PriamInstance instance = getTokenRetriever().get();
        Truth.assertThat(instance.getToken()).isEqualTo("170141183460469231731687303717692681326");
        // region offset for us-east-1 + number of racs in cluster (3)
        Truth.assertThat(instance.getId()).isEqualTo(1808575603);
    }

    @Test
    public void testNewTokenFailureWhenMyRacIsNotInCluster() {
        ((FakeConfiguration) configuration).setRacs("az2", "az3");
        create(0, "iid_0", "host_0", "127.0.0.0", "az2", 0 + "");
        create(1, "iid_1", "host_1", "127.0.0.1", "az2", 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of("iid_0", "iid_1");
            }
        };
        Assertions.assertThrows(IllegalStateException.class, () -> getTokenRetriever().get());
    }

    @Test
    public void testNewTokenGenerationMultipleInstancesWithLargetEnoughIds() throws Exception {
        create(2000000000, "iid_0", "host_0", "127.0.0.0", "az1", 0 + "");
        create(2000000001, "iid_1", "host_1", "127.0.0.1", "az1", 1 + "");
        new Expectations() {
            {
                membership.getRacMembership();
                result = ImmutableSet.of("iid_0", "iid_1");
                membership.getRacCount();
                result = 1;
                membership.getRacMembershipSize();
                result = 3;
            }
        };
        PriamInstance instance = getTokenRetriever().get();
        Truth.assertThat(instance.getToken())
                .isEqualTo("10856391546591660081525376676060033425699421368");
        // max id (2000000001) + total instances (3)
        Truth.assertThat(instance.getId()).isEqualTo(2000000004);
    }

    @Test
    public void testPreassignedTokenNotReplacedIfPublicIPMatch(@Mocked SystemUtils systemUtils)
            throws Exception {
        // IP in DB doesn't matter so we make it different to confirm that
        create(0, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 0 + "");
        getInstances(5);
        String gossipResponse = getStatus(liveInstances, tokenToEndpointMap);

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                returns(gossipResponse, gossipResponse, null, "random_value", gossipResponse);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        tokenRetriever.get();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isFalse();
    }

    @Test
    public void testPreassignedTokenNotReplacedIfPrivateIPMatch(@Mocked SystemUtils systemUtils)
            throws Exception {
        // IP in DB doesn't matter so we make it different to confirm that
        create(0, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 0 + "");
        getInstances(5);
        Map<String, String> myTokenToEndpointMap =
                IntStream.range(0, 7)
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        String::valueOf, e -> String.format("127.1.1.%s", e)));
        ImmutableList<String> myLiveInstances = ImmutableList.copyOf(tokenToEndpointMap.values());
        String gossipResponse = getStatus(myLiveInstances, myTokenToEndpointMap);

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                returns(gossipResponse, gossipResponse, null, "random_value", gossipResponse);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        tokenRetriever.get();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isFalse();
    }

    @Test
    public void testGetPreassignedTokenThrowsIfOwnerIPIsLive(@Mocked SystemUtils systemUtils)
            throws Exception {
        getInstances(5);
        create(6, instanceInfo.getInstanceId(), "host_5", "1.2.3.4", "az1", 6 + "");
        Map<String, String> myTokenToEndpointMap =
                IntStream.range(0, 7)
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        String::valueOf, e -> String.format("18.221.0.%s", e)));
        ImmutableList<String> myLiveInstances = ImmutableList.copyOf(myTokenToEndpointMap.values());
        String gossipResponse = getStatus(myLiveInstances, myTokenToEndpointMap);

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                returns(gossipResponse, gossipResponse, null, "random_value", gossipResponse);
            }
        };
        Assertions.assertThrows(
                TokenRetrieverUtils.GossipParseException.class, () -> getTokenRetriever().get());
    }

    @Test
    public void testGetPreassignedTokenReplacesIfOwnerIPIsNotLive(@Mocked SystemUtils systemUtils)
            throws Exception {
        getInstances(5);
        create(6, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 6 + "");
        Map<String, String> myTokenToEndpointMap =
                IntStream.range(0, 7)
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        String::valueOf, e -> String.format("18.221.0.%s", e)));
        List<String> myLiveInstances =
                tokenToEndpointMap.values().stream().sorted().limit(6).collect(Collectors.toList());
        String gossipResponse = getStatus(myLiveInstances, myTokenToEndpointMap);

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                returns(gossipResponse, gossipResponse, null, "random_value", gossipResponse);
            }
        };
        TokenRetriever tokenRetriever = getTokenRetriever();
        tokenRetriever.get();
        Truth.assertThat(tokenRetriever.getReplacedIp().isPresent()).isTrue();
    }

    @Test
    public void testIPIsUpdatedWhenGrabbingPreassignedToken(@Mocked SystemUtils systemUtils)
            throws Exception {
        create(0, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 0 + "");
        Truth.assertThat(getTokenRetriever().get().getHostIP()).isEqualTo("127.0.0.0");
    }

    @Test
    public void testRingPositionFirst(@Mocked SystemUtils systemUtils) throws Exception {
        getInstances(6);
        create(0, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 0 + "");
        TokenRetriever tokenRetriever = getTokenRetriever();
        tokenRetriever.get();
        Truth.assertThat(tokenRetriever.getRingPosition()).isEqualTo(Fraction.getFraction(0, 7));
    }

    @Test
    public void testRingPositionMiddle(@Mocked SystemUtils systemUtils) throws Exception {
        getInstances(3);
        create(4, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 4 + "");
        createByIndex(5);
        createByIndex(6);
        TokenRetriever tokenRetriever = getTokenRetriever();
        tokenRetriever.get();
        Truth.assertThat(tokenRetriever.getRingPosition()).isEqualTo(Fraction.getFraction(3, 6));
    }

    @Test
    public void testRingPositionLast(@Mocked SystemUtils systemUtils) throws Exception {
        getInstances(6);
        create(7, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "az1", 7 + "");
        TokenRetriever tokenRetriever = getTokenRetriever();
        tokenRetriever.get();
        Truth.assertThat(tokenRetriever.getRingPosition()).isEqualTo(Fraction.getFraction(6, 7));
    }

    @Test
    public void testThrowOnDuplicateTokenInSameRegion() {
        prepareTokenGenerationTest();
        create(1, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "us-east-1d", 1808575600 + "");
        Assert.assertThrows(
                IllegalStateException.class, () -> getTokenRetriever().generateNewToken());
    }

    @Test
    public void testIncrementDuplicateTokenInDifferentRegion() {
        ((FakeInstanceInfo) instanceInfo).setRegion("us-west-2");
        create(1, instanceInfo.getInstanceId(), "host_0", "1.2.3.4", "us-west-2a", 1808575600 + "");
        prepareTokenGenerationTest();
        Truth.assertThat(getTokenRetriever().generateNewToken().getToken()).isEqualTo("1808575601");
    }

    private void prepareTokenGenerationTest() {
        ((FakeConfiguration) configuration).setCreateNewToken(true);
        ((FakeConfiguration) configuration)
                .setPartitioner("org.apache.cassandra.dht.RandomPartitioner");
        ((FakeConfiguration) configuration).setRacs("us-east-1c", "us-east-1d", "us-east-1e");
        ((FakeInstanceInfo) instanceInfo).setRegion("us-east-1");
        ((FakeInstanceInfo) instanceInfo).setRac("us-east-1c");
        new Expectations() {
            {
                membership.getRacMembershipSize();
                result = 2;
            }
        };
        new Expectations() {
            {
                membership.getRacCount();
                result = 3;
            }
        };
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

    private TokenRetriever getTokenRetriever() {
        return new TokenRetriever(
                factory,
                membership,
                configuration,
                instanceInfo,
                new FakeSleeper(),
                new TokenManager(configuration));
    }
}
