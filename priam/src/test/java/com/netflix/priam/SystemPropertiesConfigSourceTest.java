package com.netflix.priam;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SystemPropertiesConfigSourceTest 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemPropertiesConfigSourceTest.class.getName());

    @Test
    public void read() 
    {
        final String key = "java.version";
        SystemPropertiesConfigSource configSource = new SystemPropertiesConfigSource();
        configSource.initialize("asgName", "region");

        // sys props are filtered to starting with priam, so this should be missing.
        Assert.assertEquals(null, configSource.get(key));

        Assert.assertEquals(0, configSource.size());
    }
}
