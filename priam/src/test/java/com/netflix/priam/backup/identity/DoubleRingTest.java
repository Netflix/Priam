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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.netflix.priam.identity.DoubleRing;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import java.util.List;
import org.junit.Test;

public class DoubleRingTest extends InstanceTestUtils {

    @Test
    public void testDouble() throws Exception {
        createInstances();
        int originalSize = factory.getAllIds(config.getAppName()).size();
        new DoubleRing(config, factory, tokenManager, instanceInfo).doubleSlots();
        ImmutableSet<PriamInstance> doubled = factory.getAllIds(config.getAppName());
        assertEquals(originalSize * 2, doubled.size());
        validate(doubled.asList());
    }

    private void validate(List<PriamInstance> doubled) {
        List<String> validator = Lists.newArrayList();
        for (int i = 0; i < doubled.size(); i++) {
            validator.add(tokenManager.createToken(i, doubled.size(), instanceInfo.getRegion()));
        }

        for (int i = 0; i < doubled.size(); i++) {
            PriamInstance ins = doubled.get(i);
            assertEquals(validator.get(i), ins.getToken());
            int id = ins.getId() - tokenManager.regionOffset(instanceInfo.getRegion());
            System.out.println(ins);
            if (0 != id % 2) assertEquals(ins.getInstanceId(), InstanceIdentity.DUMMY_INSTANCE_ID);
        }
    }

    @Test
    public void testBR() throws Exception {
        createInstances();
        int intialSize = factory.getAllIds(config.getAppName()).size();
        DoubleRing ring = new DoubleRing(config, factory, tokenManager, instanceInfo);
        ring.backup();
        ring.doubleSlots();
        assertEquals(intialSize * 2, factory.getAllIds(config.getAppName()).size());
        ring.restore();
        assertEquals(intialSize, factory.getAllIds(config.getAppName()).size());
    }
}
