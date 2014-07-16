package com.netflix.priam;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PropertiesConfigSourceTest 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesConfigSourceTest.class.getName());

    @Test
    public void readFile() 
    {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/Priam.properties");
        configSource.initialize("asgName", "region");

        Assert.assertEquals("\"/tmp/commitlog\"", configSource.get("Priam.backup.commitlog.location"));
        Assert.assertEquals(7102, configSource.get("Priam.thrift.port", 0));
        // File has 12 lines, but line 6 is "Priam.jmx.port7501", so it gets filtered out with empty string check.
        Assert.assertEquals(11, configSource.size());
    }

    @Test
    public void updateKey() 
    {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/Priam.properties");
        configSource.initialize("asgName", "region");

        // File has 12 lines, but line 6 is "Priam.jmx.port7501", so it gets filtered out with empty string check.
        Assert.assertEquals(11, configSource.size());

        configSource.set("foo", "bar");

        Assert.assertEquals(12, configSource.size());

        Assert.assertEquals("bar", configSource.get("foo"));

        Assert.assertEquals(7102, configSource.get("Priam.thrift.port", 0));
        configSource.set("Priam.thrift.port", Integer.toString(10));
        Assert.assertEquals(10, configSource.get("Priam.thrift.port", 0));
    }
}
