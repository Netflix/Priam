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

package com.netflix.priam.identity;

import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.FakeMembership;
import com.netflix.priam.FakePriamInstanceFactory;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceEnvIdentity;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.token.DeadTokenRetriever;
import com.netflix.priam.identity.token.NewTokenRetriever;
import com.netflix.priam.identity.token.PreGeneratedTokenRetriever;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;

import org.junit.Before;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.List;

@Ignore
public abstract class InstanceTestUtils
{

    List<String> instances = new ArrayList<String>();
    IMembership membership;
    FakeConfiguration config;
    IPriamInstanceFactory factory;
    InstanceIdentity identity;
    Sleeper sleeper;
    DeadTokenRetriever deadTokenRetriever;
    PreGeneratedTokenRetriever preGeneratedTokenRetriever;
	NewTokenRetriever newTokenRetriever;
  	ITokenManager tokenManager;
  	InstanceEnvIdentity insEnvIdentity;  

    @Before
    public void setup()
    {
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
        config = new FakeConfiguration("fake", "fake-app", "az1", "fakeinstance1");
        tokenManager = new TokenManager(config);
        factory = new FakePriamInstanceFactory(config);
        sleeper = new FakeSleeper();
        this.deadTokenRetriever = new DeadTokenRetriever(factory, membership, config, sleeper, insEnvIdentity);
        this.preGeneratedTokenRetriever = new PreGeneratedTokenRetriever(factory, membership, config, sleeper);
        this.newTokenRetriever = new NewTokenRetriever(factory, membership, config, sleeper, tokenManager);
    }

    public void createInstances() throws Exception
    {
        createInstanceIdentity("az1", "fakeinstance1");
        createInstanceIdentity("az1", "fakeinstance2");
        createInstanceIdentity("az1", "fakeinstance3");
        // try next region
        createInstanceIdentity("az2", "fakeinstance4");
        createInstanceIdentity("az2", "fakeinstance5");
        createInstanceIdentity("az2", "fakeinstance6");
        // next region
        createInstanceIdentity("az3", "fakeinstance7");
        createInstanceIdentity("az3", "fakeinstance8");
        createInstanceIdentity("az3", "fakeinstance9");
    }

    protected InstanceIdentity createInstanceIdentity(String zone, String instanceId) throws Exception
    {
        config.zone = zone;
        config.instance_id = instanceId;
        return new InstanceIdentity(factory, membership, config, sleeper, new TokenManager(config)
        , this.deadTokenRetriever
        , this.preGeneratedTokenRetriever
        , this.newTokenRetriever
        );
    }
}
