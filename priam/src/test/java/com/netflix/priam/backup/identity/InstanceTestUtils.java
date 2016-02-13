package com.netflix.priam.backup.identity;

import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.FakeMembership;
import com.netflix.priam.FakePriamInstanceFactory;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.FakeSleeper;
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
        config = new FakeConfiguration("fake", "_suffix", "fake-app", "az1", "fakeinstance1");
        factory = new FakePriamInstanceFactory(config);
        sleeper = new FakeSleeper();
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
        return new InstanceIdentity(factory, membership, config, sleeper, new TokenManager());
    }
}
