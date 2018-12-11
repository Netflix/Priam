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
        Map<String, String> map = priamHelperFunctions.parseParams("priam.abc=priam.abc", true);
        Assert.assertEquals(1, map.size());
        Assert.assertTrue(map.containsKey("priam.abc"));
        Assert.assertTrue(map.get("priam.abc").equals("1234"));
    }

    @Test
    @TestPropertyOverride({"priam.abc=1234", "priam.cass.new.config=123"})
    public void testHelperFunctionMultiValid() {
        Map<String, String> map =
                priamHelperFunctions.parseParams(
                        "priam.abc=priam.abc,priam.cass.new.config=cassandra_new_config", true);
        Assert.assertEquals(2, map.size());
        Assert.assertTrue(map.containsKey("priam.abc"));
        Assert.assertTrue(map.get("priam.abc").equals("1234"));
        Assert.assertTrue(map.containsKey("cassandra_new_config"));
        Assert.assertTrue(map.get("cassandra_new_config").equals("123"));

        map =
                priamHelperFunctions.parseParams(
                        "priam.abc=priam.abc,priam.cass.new.config=cassandra_new_config", false);
        Assert.assertEquals(2, map.size());
        Assert.assertTrue(map.containsKey("priam.cass.new.config"));
        Assert.assertTrue(map.get("priam.cass.new.config").equals("cassandra_new_config"));
    }

    @Test
    public void testEmpty() {
        Map<String, String> map = priamHelperFunctions.parseParams(null, true);
        Assert.assertTrue(map.size() == 0);
    }
}
