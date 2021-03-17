/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.priam.backup.identity;

import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.identity.*;
import com.netflix.priam.identity.config.FakeInstanceInfo;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.identity.token.*;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Ignore;

@Ignore
abstract class InstanceTestUtils {

    private final List<String> instances = new ArrayList<>();
    private IMembership membership;
    FakeConfiguration config;
    IPriamInstanceFactory factory;
    InstanceIdentity identity;
    private Sleeper sleeper;
    ITokenManager tokenManager;
    InstanceInfo instanceInfo;
    private final String region = "us-east-1";

    @Before
    public void setup() throws Exception {
        instances.add("fakeinstance1");
        instances.add("fakeinstance2");
        instances.add("fakeinstance3");
        instances.add("fakeinstance4");
        instances.add("fakeinstance5");
        instances.add("fakeinstance6");
        instances.add("fakeinstance7");
        instances.add("fakeinstance8");
        instances.add("fakeinstance9");

        membership = new FakeMembership(instances);
        config = new FakeConfiguration("fake-app");
        instanceInfo = new FakeInstanceInfo("fakeinstance1", "az1", region);
        tokenManager = new TokenManager(config);
        factory = new FakePriamInstanceFactory(instanceInfo);
        sleeper = new FakeSleeper();
        identity = createInstanceIdentity(instanceInfo.getRac(), instanceInfo.getInstanceId());
    }

    void createInstances() throws Exception {
        createInstanceIdentity("az1", "fakeinstance1");
        createInstanceIdentity("az1", "fakeinstance2");
        createInstanceIdentity("az1", "fakeinstance3");
        // try next zone
        createInstanceIdentity("az2", "fakeinstance4");
        createInstanceIdentity("az2", "fakeinstance5");
        createInstanceIdentity("az2", "fakeinstance6");
        // next zone
        createInstanceIdentity("az3", "fakeinstance7");
        createInstanceIdentity("az3", "fakeinstance8");
        createInstanceIdentity("az3", "fakeinstance9");
    }

    InstanceIdentity createInstanceIdentity(String zone, String instanceId) throws Exception {
        InstanceInfo newInstanceInfo = new FakeInstanceInfo(instanceId, zone, region);
        ITokenRetriever tokenRetriever =
                new TokenRetriever(
                        factory, membership, config, newInstanceInfo, sleeper, tokenManager);
        return new InstanceIdentity(factory, membership, config, newInstanceInfo, tokenRetriever);
    }
}
