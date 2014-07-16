package com.netflix.priam;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompositeConfigSourceTest 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeConfigSourceTest.class.getName());

    @Test
    public void read() 
    {
        MemoryConfigSource memoryConfigSource = new MemoryConfigSource();
        IConfigSource configSource = new CompositeConfigSource(memoryConfigSource);
        configSource.initialize("foo", "bar");

        Assert.assertEquals(0, configSource.size());
        configSource.set("foo", "bar");
        Assert.assertEquals(1, configSource.size());
        Assert.assertEquals("bar", configSource.get("foo"));

        // verify that the writes went to mem source.
        Assert.assertEquals(1, memoryConfigSource.size());
        Assert.assertEquals("bar", memoryConfigSource.get("foo"));
    }

    @Test
    public void readMultiple() 
    {
        MemoryConfigSource m1 = new MemoryConfigSource();
        m1.set("foo", "foo");
        MemoryConfigSource m2 = new MemoryConfigSource();
        m2.set("bar", "bar");
        MemoryConfigSource m3 = new MemoryConfigSource();
        m3.set("baz", "baz");

        IConfigSource configSource = new CompositeConfigSource(m1, m2, m3);
        Assert.assertEquals(3, configSource.size());
        Assert.assertEquals("foo", configSource.get("foo"));
        Assert.assertEquals("bar", configSource.get("bar"));
        Assert.assertEquals("baz", configSource.get("baz"));

        // read default
        Assert.assertEquals("test", configSource.get("doesnotexist", "test"));
    }
}
