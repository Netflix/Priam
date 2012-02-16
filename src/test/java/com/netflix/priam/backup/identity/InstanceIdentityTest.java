package com.netflix.priam.backup.identity;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.SystemUtils;

public class InstanceIdentityTest extends InstanceTestUtils
{

    @Test
    public void testCreateToken() throws Exception
    {

        identity = new InstanceIdentity(factory, membership, config);
        int hash = SystemUtils.hash(config.getDC());
        assertEquals(0, identity.getInstance().getId() - hash);

        config.zone = "az1";
        config.instance_id = "fakeinstance2";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(3, identity.getInstance().getId() - hash);

        config.zone = "az1";
        config.instance_id = "fakeinstance3";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(6, identity.getInstance().getId() - hash);

        // try next region
        config.zone = "az2";
        config.instance_id = "fakeinstance4";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(1, identity.getInstance().getId() - hash);

        config.zone = "az2";
        config.instance_id = "fakeinstance5";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(4, identity.getInstance().getId() - hash);

        config.zone = "az2";
        config.instance_id = "fakeinstance6";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(7, identity.getInstance().getId() - hash);

        // next
        config.zone = "az3";
        config.instance_id = "fakeinstance7";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(2, identity.getInstance().getId() - hash);

        config.zone = "az3";
        config.instance_id = "fakeinstance8";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(5, identity.getInstance().getId() - hash);

        config.zone = "az3";
        config.instance_id = "fakeinstance9";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(8, identity.getInstance().getId() - hash);
    }

    @Test
    public void testDeadInstance() throws Exception
    {
        createInstances();
        instances.remove("fakeinstance4");
        config.zone = "az2";
        config.instance_id = "fakeinstancex";
        identity = new InstanceIdentity(factory, membership, config);
        int hash = SystemUtils.hash(config.getDC());
        assertEquals(1, identity.getInstance().getId() - hash);
    }

    @Test
    public void testGetSeeds() throws Exception
    {
        createInstances();
        config.zone = "az1";
        config.instance_id = "fakeinstancex";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(3, identity.getSeeds().size());

        config.zone = "az1";
        config.instance_id = "fakeinstance1";
        identity = new InstanceIdentity(factory, membership, config);
        assertEquals(2, identity.getSeeds().size());
    }

    @Test
    public void testDoubleSlots() throws Exception
    {
        createInstances();
        int before = factory.getAllIds("fake-app").size();
        new DoubleRing(config, factory).doubleSlots();
        List<PriamInstance> lst = factory.getAllIds(config.getAppName());
        // sort it so it will look good if you want to print it.
        factory.sort(lst);
        for (int i = 0; i < lst.size(); i++)
        {
            System.out.println(lst.get(i));
            if (0 == i % 2)
                continue;
            assertEquals("new_slot", lst.get(i).getInstanceId());
        }
        assertEquals(before * 2, lst.size());
    }

    @Test
    public void testDoubleGrap() throws Exception
    {
        createInstances();
        new DoubleRing(config, factory).doubleSlots();
        config.zone = "az1";
        config.instance_id = "fakeinstancex";
        int hash = SystemUtils.hash(config.getDC());
        identity = new InstanceIdentity(factory, membership, config);
        printInstance(identity.getInstance(), hash);
    }

    public void printInstance(PriamInstance ins, int hash)
    {
        System.out.println("ID: " + (ins.getId() - hash));
        System.out.println("PayLoad: " + ins.getToken());

    }

}
