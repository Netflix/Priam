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

package com.netflix.priam.configSource;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PropertiesConfigSourceTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PropertiesConfigSourceTest.class.getName());

    @Test
    public void readFile() {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/Priam.properties");
        configSource.initialize("asgName", "region");

        Assert.assertEquals(
                "\"/tmp/commitlog\"", configSource.get("Priam.backup.commitlog.location"));
        Assert.assertEquals(7102, configSource.get("Priam.thrift.port", 0));
        // File has 13 lines, but line 6 is "Priam.jmx.port7501", so it gets filtered out with empty
        // string check.
        Assert.assertEquals(13, configSource.size());
    }

    @Test
    public void updateKey() {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/Priam.properties");
        configSource.initialize("asgName", "region");

        // File has 13 lines, but line 6 is "Priam.jmx.port7501", so it gets filtered out with empty
        // string check.
        Assert.assertEquals(13, configSource.size());

        configSource.set("foo", "bar");

        Assert.assertEquals(14, configSource.size());

        Assert.assertEquals("bar", configSource.get("foo"));

        Assert.assertEquals(7102, configSource.get("Priam.thrift.port", 0));
        configSource.set("Priam.thrift.port", Integer.toString(10));
        Assert.assertEquals(10, configSource.get("Priam.thrift.port", 0));
    }
}
