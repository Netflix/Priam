package com.netflix.priam.backup.identity;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.TokenManager;
import static org.junit.Assert.assertEquals;

public class DoubleRingTest extends InstanceTestUtils
{

    @Test
    public void testDouble() throws Exception
    {
        createInstances();
        int originalSize = factory.getAllIds(config.getAppName()).size();
        new DoubleRing(config, factory).doubleSlots();
        List<PriamInstance> doubled = factory.getAllIds(config.getAppName());
        factory.sort(doubled);

        assertEquals(originalSize * 2, doubled.size());
        validate(doubled);
    }

    private void validate(List<PriamInstance> doubled)
    {
        List<String> validator = Lists.newArrayList();
        for (int i = 0; i < doubled.size(); i++)
        {
            validator.add(TokenManager.createToken(i, doubled.size(), config.getDC()));
            
        }
        
        for (int i = 0; i < doubled.size(); i++)
        {
            PriamInstance ins = doubled.get(i);
            assertEquals(validator.get(i), ins.getToken());
            int id = ins.getId() - TokenManager.regionOffset(config.getDC());
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
        DoubleRing ring = new DoubleRing(config, factory);
        ring.backup();
        ring.doubleSlots();
        assertEquals(intialSize * 2, factory.getAllIds(config.getAppName()).size());
        ring.restore();
        assertEquals(intialSize, factory.getAllIds(config.getAppName()).size());
    }
}
