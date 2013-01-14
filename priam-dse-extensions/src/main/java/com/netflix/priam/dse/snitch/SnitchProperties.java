package com.netflix.priam.dse.snitch;

import java.io.InputStream;
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;

public class SnitchProperties
{
    public static final String RACKDC_PROPERTY_FILENAME = "cassandra-rackdc.properties";
    private static Properties properties = new Properties();

    static
    {
        InputStream stream = SnitchProperties.class.getClassLoader().getResourceAsStream(RACKDC_PROPERTY_FILENAME);
        try
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to read " + RACKDC_PROPERTY_FILENAME, e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Get a snitch property value or return null if not defined.
     */
    public static String get(String prop)
    {
        return properties.getProperty(prop, null);
    }
}
