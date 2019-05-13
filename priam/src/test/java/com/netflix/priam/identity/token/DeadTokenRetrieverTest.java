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

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.SystemUtils;
import java.util.List;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 3/1/19. */
public class DeadTokenRetrieverTest {
    @Mocked private IPriamInstanceFactory<PriamInstance> factory;
    @Mocked private IMembership membership;
    private IDeadTokenRetriever deadTokenRetriever;
    private InstanceInfo instanceInfo;
    private IConfiguration configuration;

    public DeadTokenRetrieverTest() {
        Injector injector = Guice.createInjector(new BRTestModule());
        if (instanceInfo == null) instanceInfo = injector.getInstance(InstanceInfo.class);
        if (configuration == null) configuration = injector.getInstance(IConfiguration.class);
    }

    @Test
    public void testNoReplacementNormalScenario() throws Exception {
        new Expectations() {
            {
                factory.getAllIds(anyString);
                result = Lists.newArrayList();
                membership.getRacMembership();
                result = Lists.newArrayList();
            }
        };
        deadTokenRetriever =
                new DeadTokenRetriever(
                        factory, membership, configuration, new FakeSleeper(), instanceInfo);
        PriamInstance priamInstance = deadTokenRetriever.get();
        Assert.assertNull(priamInstance);
    }

    @Test
    // There is no slot available for replacement as per Token Database.
    public void testNoReplacementNoSpotAvailable() throws Exception {
        List<PriamInstance> allInstances = getInstances(1);
        List<String> racMembership = getRacMembership(1);
        racMembership.add(instanceInfo.getInstanceId());

        new Expectations() {
            {
                factory.getAllIds(anyString);
                result = allInstances;
                membership.getRacMembership();
                result = racMembership;
            }
        };
        deadTokenRetriever =
                new DeadTokenRetriever(
                        factory, membership, configuration, new FakeSleeper(), instanceInfo);
        PriamInstance priamInstance = deadTokenRetriever.get();
        Assert.assertNull(priamInstance);
        new Verifications() {
            {
                factory.delete(withAny(priamInstance));
                times = 0;
            }
        };
    }

    @Test
    // There is a potential slot for dead token but we are unable to replace.
    public void testNoReplacementNoGossipMatch(@Mocked SystemUtils systemUtils) throws Exception {
        List<PriamInstance> allInstances = getInstances(2);
        List<String> racMembership = getRacMembership(1);
        racMembership.add(instanceInfo.getInstanceId());
        PriamInstance instance = null;
        // gossip info returns null, thus unable to replace the instance.
        new Expectations() {
            {
                factory.getAllIds(anyString);
                result = allInstances;
                membership.getRacMembership();
                result = racMembership;
                SystemUtils.getDataFromUrl(anyString);
                result =
                        "[{\"TOKENS\":\"[1]\",\"PUBLIC_IP\":\"\",\"RACK\":\"az1\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[2]\",\"PUBLIC_IP\":\"\",\"RACK\":\"az1\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"}]";
                times = 1;
            }
        };
        deadTokenRetriever =
                new DeadTokenRetriever(
                        factory, membership, configuration, new FakeSleeper(), instanceInfo);
        PriamInstance priamInstance = deadTokenRetriever.get();
        Assert.assertNull(priamInstance);
        new Verifications() {
            {
                factory.delete(withAny(instance));
                times = 1;
            }
        };

        // gossip info from another instance returns everything is OK and thus unable to replace.
        String gossipInfo =
                "[{\"TOKENS\":\"[1]\",\"PUBLIC_IP\":\"127.0.0.1\",\"RACK\":\"az1\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[2]\",\"PUBLIC_IP\":\"127.0.0.2\",\"RACK\":\"az1\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"}]";
        new Expectations() {
            {
                factory.getAllIds(anyString);
                result = allInstances;
                membership.getRacMembership();
                result = racMembership;
                SystemUtils.getDataFromUrl(anyString);
                result = gossipInfo;
                times = 1;
            }
        };
        priamInstance = deadTokenRetriever.get();
        Assert.assertNull(priamInstance);
        new Verifications() {
            {
                factory.delete(withAny(instance));
                times = 1;
            }
        };
    }

    @Test
    public void testReplacementGossipMatch(@Mocked SystemUtils systemUtils) throws Exception {
        List<PriamInstance> allInstances = getInstances(6);
        List<String> racMembership = getRacMembership(2);
        racMembership.add(instanceInfo.getInstanceId());
        String gossipResponse =
                "[{\"TOKENS\":\"[1]\",\"PUBLIC_IP\":\"127.0.0.1\",\"RACK\":\"az1\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[2]\",\"PUBLIC_IP\":\"127.0.0.2\",\"RACK\":\"az1\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[3]\",\"PUBLIC_IP\":\"127.0.0.3\",\"RACK\":\"az1\",\"STATUS\":\"shutdown\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[4]\",\"PUBLIC_IP\":\"127.0.0.4\",\"RACK\":\"az2\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[5]\",\"PUBLIC_IP\":\"127.0.0.5\",\"RACK\":\"az2\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"},{\"TOKENS\":\"[6]\",\"PUBLIC_IP\":\"127.0.0.6\",\"RACK\":\"az2\",\"STATUS\":\"NORMAL\",\"DC\":\"us-east-1\"}]";
        new Expectations() {
            {
                factory.getAllIds(anyString);
                result = allInstances;
                membership.getRacMembership();
                result = racMembership;
                SystemUtils.getDataFromUrl(anyString);
                returns(gossipResponse, gossipResponse, null, gossipResponse);
            }
        };
        deadTokenRetriever =
                new DeadTokenRetriever(
                        factory, membership, configuration, new FakeSleeper(), instanceInfo);
        PriamInstance priamInstance = deadTokenRetriever.get();
        Assert.assertNotNull(priamInstance);
        Assert.assertEquals("127.0.0.3", deadTokenRetriever.getReplaceIp());
    }

    private List<PriamInstance> getInstances(int noOfInstances) {
        List<PriamInstance> allInstances = Lists.newArrayList();
        for (int i = 1; i <= noOfInstances; i++)
            allInstances.add(
                    create(
                            i,
                            String.format("instance_id_%d", i),
                            String.format("hostname_%d", i),
                            String.format("127.0.0.%d", i),
                            instanceInfo.getRac(),
                            i + ""));
        return allInstances;
    }

    private List<String> getRacMembership(int noOfInstances) {
        List<String> racMembership = Lists.newArrayList();
        for (int i = 1; i <= noOfInstances; i++)
            racMembership.add(String.format("instance_id_%d", i));
        return racMembership;
    }

    private PriamInstance create(
            int id, String instanceID, String hostname, String ip, String rac, String payload) {
        PriamInstance ins = new PriamInstance();
        ins.setApp(configuration.getAppName());
        ins.setRac(rac);
        ins.setHost(hostname);
        ins.setHostIP(ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setDC(instanceInfo.getRegion());
        ins.setToken(payload);
        return ins;
    }
}
