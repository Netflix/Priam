package com.netflix.backup.identity;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.priam.identity.DoubleRing;
import com.priam.identity.PriamInstance;
import com.priam.utils.SystemUtils;

public class DoubleRingTest extends InstanceTestUtils
{

    @Test
    public void testDouble() throws Exception
    {
        createInstances();
        int originalSize = factory.getAllIds(config.getAppName()).size();
        new DoubleRing(config, factory, membership).doubleSlots();
        List<PriamInstance> doubled = factory.getAllIds(config.getAppName());
        factory.sort(doubled);

        assertEquals(originalSize * 2, doubled.size());
        validate(doubled);
    }

    private void validate(List<PriamInstance> doubled)
    {
        for (PriamInstance ins : doubled)
        {
            int id = ins.getId() - SystemUtils.hash(config.getDC());
            System.out.println(ins);
            if (0 != id % 2)
                assertEquals(ins.getInstanceId(), "new_slot");
        }
    }

    @Test
    public void testBR() throws Exception
    {
        createInstances();
        int intialSize = factory.getAllIds(config.getAppName()).size();
        DoubleRing ring = new DoubleRing(config, factory, membership);
        ring.backup();
        ring.doubleSlots();
        assertEquals(intialSize * 2, factory.getAllIds(config.getAppName()).size());
        ring.restore();
        assertEquals(intialSize, factory.getAllIds(config.getAppName()).size());
    }
}
