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

package com.netflix.priam.config;

import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.TestPropertyOverride;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.identity.config.InstanceInfo;
import javax.inject.Inject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Verifies that system properties may be used to set values returned by dynamic proxies generated
 * from {@link com.netflix.priam.config.IConfiguration},using the namespace prefix "priam."
 */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({BRTestModule.class, ArchaiusModule.class})
@TestPropertyOverride({"priam.partitioner=CustomPartitioner"})
public class ConfigurationPropertiesTest {

    @Inject IConfiguration config;
    @Inject InstanceInfo instanceInfo;

    @Test
    public void testInvokingProcessMethodOnPartitioner() throws Exception {
        Assert.assertEquals("CustomPartitioner", config.getPartitioner());
    }

    @Test
    public void testDefaults() {
        Assert.assertEquals(9160, config.getThriftPort());
        Assert.assertEquals("fake-app", config.getAppName());
    }

    @Test
    @TestPropertyOverride({"priam.thrift.port=1234"})
    public void testMethodOverrides() {
        Assert.assertEquals(1234, config.getThriftPort());
    }

    @Test
    public void testObjects() {
        Assert.assertEquals("us-east-1", instanceInfo.getRegion());
    }
}
