package com.priam.backup.identity;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;

import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.priam.FakeConfiguration;
import com.priam.FakeMembership;
import com.priam.FakePriamInstanceFactory;

@Ignore
public abstract class InstanceTestUtils
{

    List<String> instances = new ArrayList<String>();
    IMembership membership;
    FakeConfiguration config;
    IPriamInstanceFactory factory;
    InstanceIdentity identity;

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
        factory = new FakePriamInstanceFactory(config);
    }

    public void createInstances() throws Exception
    {
        identity = new InstanceIdentity(factory, membership, config);

        config.zone = "az1";
        config.instance_id = "fakeinstance2";
        identity = new InstanceIdentity(factory, membership, config);

        config.zone = "az1";
        config.instance_id = "fakeinstance3";
        identity = new InstanceIdentity(factory, membership, config);

        // try next region
        config.zone = "az2";
        config.instance_id = "fakeinstance4";
        identity = new InstanceIdentity(factory, membership, config);

        config.zone = "az2";
        config.instance_id = "fakeinstance5";
        identity = new InstanceIdentity(factory, membership, config);

        config.zone = "az2";
        config.instance_id = "fakeinstance6";
        identity = new InstanceIdentity(factory, membership, config);

        // next
        config.zone = "az3";
        config.instance_id = "fakeinstance7";
        identity = new InstanceIdentity(factory, membership, config);

        config.zone = "az3";
        config.instance_id = "fakeinstance8";
        identity = new InstanceIdentity(factory, membership, config);

        config.zone = "az3";
        config.instance_id = "fakeinstance9";
        identity = new InstanceIdentity(factory, membership, config);
    }

}
