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

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import org.junit.Test;

public class InstanceIdentityTest extends InstanceTestUtils {

    @Test
    public void testCreateToken() throws Exception {

        identity = createInstanceIdentity("az1", "fakeinstance1");
        int hash = tokenManager.regionOffset(instanceInfo.getRegion());
        assertEquals(0, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az1", "fakeinstance2");
        assertEquals(3, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az1", "fakeinstance3");
        assertEquals(6, identity.getInstance().getId() - hash);

        // try next zone
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
    public void testGetSeeds() throws Exception {
        createInstances();
        identity = createInstanceIdentity("az1", "fakeinstance1");
        assertEquals(3, identity.getSeeds().size());
    }

    @Test
    public void testDoubleSlots() throws Exception {
        createInstances();
        int before = factory.getAllIds(config.getAppName()).size();
        new DoubleRing(config, factory, tokenManager, instanceInfo).doubleSlots();
        ImmutableList<PriamInstance> lst = factory.getAllIds(config.getAppName()).asList();
        for (int i = 0; i < lst.size(); i++) {
            System.out.println(lst.get(i));
            if (0 == i % 2) continue;
            assertEquals(InstanceIdentity.DUMMY_INSTANCE_ID, lst.get(i).getInstanceId());
        }
        assertEquals(before * 2, lst.size());
    }

    @Test
    public void testDoubleGrap() throws Exception {
        createInstances();
        new DoubleRing(config, factory, tokenManager, instanceInfo).doubleSlots();
        int hash = tokenManager.regionOffset(instanceInfo.getRegion());
        identity = createInstanceIdentity("az1", "fakeinstancex");
        printInstance(identity.getInstance(), hash);
    }

    public void printInstance(PriamInstance ins, int hash) {
        System.out.println("ID: " + (ins.getId() - hash));
        System.out.println("PayLoad: " + ins.getToken());
    }
}
