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

public final class SystemPropertiesConfigSourceTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SystemPropertiesConfigSourceTest.class.getName());

    @Test
    public void read() {
        final String key = "java.version";
        SystemPropertiesConfigSource configSource = new SystemPropertiesConfigSource();
        configSource.initialize("asgName", "region");

        // sys props are filtered to starting with priam, so this should be missing.
        Assert.assertEquals(null, configSource.get(key));

        Assert.assertEquals(0, configSource.size());
    }
}
