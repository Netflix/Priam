package com.netflix.priam.backup.identity;

import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.TokenManager;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class InstanceIdentityTest extends InstanceTestUtils
{
    private static final ITokenManager tokenManager = new TokenManager();

    @Test
    public void testCreateToken() throws Exception
    {

        identity = createInstanceIdentity("az1", "fakeinstance1");
        int hash = tokenManager.dcOffset(config.getDC());
        assertEquals(0, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az1", "fakeinstance2");
        assertEquals(3, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az1", "fakeinstance3");
        assertEquals(6, identity.getInstance().getId() - hash);

        // try next region
        identity = createInstanceIdentity("az2", "fakeinstance4");
        assertEquals(1, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az2", "fakeinstance5");
        assertEquals(4, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az2", "fakeinstance6");
        assertEquals(7, identity.getInstance().getId() - hash);

        // next
        identity = createInstanceIdentity("az3", "fakeinstance7");
        assertEquals(2, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az3", "fakeinstance8");
        assertEquals(5, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az3", "fakeinstance9");
        assertEquals(8, identity.getInstance().getId() - hash);
    }
    
    @Test
    public void testGetSeeds() throws Exception
    {
        createInstances();
        identity = createInstanceIdentity("az1", "fakeinstance1");
        assertEquals(3, identity.getSeeds().size());
    }

    @Test
    public void testDoubleSlots() throws Exception
    {
        createInstances();
        int before = factory.getAllIds("fake-app").size();
        new DoubleRing(config, factory, tokenManager).doubleSlots();
        List<PriamInstance> lst = factory.getAllIds(config.getAppName());
        // sort it so it will look good if you want to print it.
        factory.sort(lst);
        for (int i = 0; i < lst.size(); i++)
        {
            System.out.println(lst.get(i));
            if (0 == i % 2)
                continue;
            assertEquals(InstanceIdentity.DUMMY_INSTANCE_ID, lst.get(i).getInstanceId());
        }
        assertEquals(before * 2, lst.size());
    }

    @Test
    public void testDoubleGrap() throws Exception
    {
        createInstances();
        new DoubleRing(config, factory, tokenManager).doubleSlots();
        config.zone = "az1";
        config.instance_id = "fakeinstancex";
        int hash = tokenManager.dcOffset(config.getDC());
        identity = createInstanceIdentity("az1", "fakeinstancex");
        printInstance(identity.getInstance(), hash);
    }

    public void printInstance(PriamInstance ins, int hash)
    {
        System.out.println("ID: " + (ins.getId() - hash));
        System.out.println("PayLoad: " + ins.getToken());

    }

}
