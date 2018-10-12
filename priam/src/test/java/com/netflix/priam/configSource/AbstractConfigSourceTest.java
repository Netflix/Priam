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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AbstractConfigSourceTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractConfigSourceTest.class.getName());

    @Test
    public void lists() {
        AbstractConfigSource source = new MemoryConfigSource();
        source.set("foo", "bar,baz, qux ");
        final List<String> values = source.getList("foo");
        LOGGER.info("Values {}", values);
        Assert.assertEquals(ImmutableList.of("bar", "baz", "qux"), values);
    }

    @Test
    public void oneItem() {
        AbstractConfigSource source = new MemoryConfigSource();
        source.set("foo", "bar");
        final List<String> values = source.getList("foo");
        LOGGER.info("Values {}", values);
        Assert.assertEquals(ImmutableList.of("bar"), values);
    }

    @Test
    public void oneItemWithSpace() {
        AbstractConfigSource source = new MemoryConfigSource();
        source.set("foo", "\tbar ");
        final List<String> values = source.getList("foo");
        LOGGER.info("Values {}", values);
        Assert.assertEquals(ImmutableList.of("bar"), values);
    }
}
