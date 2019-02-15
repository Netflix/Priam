/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.priam.utils;

import com.google.inject.Inject;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.TestPropertyOverride;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.backup.BRTestModule;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Created by aagrawal on 11/2/18. */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
public class TestPriamHelperFunctions {
    @Inject private PriamHelperFunctions priamHelperFunctions;

    @Test
    @TestPropertyOverride({"priam.abc=1234"})
    public void testHelperFunctionValid() {
        Map<String, String> map = new HashMap<>();
        priamHelperFunctions.parseParams(map, "priam.abc=some_cass_key");
        Assert.assertEquals(1, map.size());
        Assert.assertTrue(map.containsKey("some_cass_key"));
        Assert.assertTrue(map.get("some_cass_key").equals("1234"));
    }

    @Test
    @TestPropertyOverride({"priam.abc=1234", "priam.cass.new.config=123", "priam.value=random"})
    public void testHelperFunctionMultiValid() {
        Map<String, String> map = new HashMap<>();
        priamHelperFunctions.parseParams(
                map, "priam.abc=some_cass_key,priam.cass.new.config=cassandra_new_config");
        Assert.assertEquals(2, map.size());
        Assert.assertTrue(map.containsKey("some_cass_key"));
        Assert.assertTrue(map.get("some_cass_key").equals("1234"));
        Assert.assertTrue(map.containsKey("cassandra_new_config"));
        Assert.assertTrue(map.get("cassandra_new_config").equals("123"));

        Map map1 = new HashMap();
        priamHelperFunctions.parseParams(map1, "priam.value=cassandra.option");
        Assert.assertEquals(1, map1.size());
        Assert.assertTrue(map1.containsKey("cassandra"));
        Assert.assertTrue(((Map) map1.get("cassandra")).get("option").equals("random"));
    }

    @Test
    public void testEmpty() {
        Map<String, String> map = new HashMap<>();
        priamHelperFunctions.parseParams(map, null);
        Assert.assertTrue(map.size() == 0);
    }
}
