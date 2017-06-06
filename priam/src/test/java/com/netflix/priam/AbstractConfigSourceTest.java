package com.netflix.priam;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class AbstractConfigSourceTest 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigSourceTest.class.getName());

    @Test
    public void lists() 
    {
        AbstractConfigSource source = new MemoryConfigSource();
        source.set("foo", "bar,baz, qux ");
        final List<String> values = source.getList("foo");
        LOGGER.info("Values {}", values);
        Assert.assertEquals(ImmutableList.of("bar", "baz", "qux"), values);
    }

    @Test
    public void oneItem() 
    {
        AbstractConfigSource source = new MemoryConfigSource();
        source.set("foo", "bar");
        final List<String> values = source.getList("foo");
        LOGGER.info("Values {}", values);
        Assert.assertEquals(ImmutableList.of("bar"), values);
    }

    @Test
    public void oneItemWithSpace() 
    {
        AbstractConfigSource source = new MemoryConfigSource();
        source.set("foo", "\tbar ");
        final List<String> values = source.getList("foo");
        LOGGER.info("Values {}", values);
        Assert.assertEquals(ImmutableList.of("bar"), values);
    }
}
