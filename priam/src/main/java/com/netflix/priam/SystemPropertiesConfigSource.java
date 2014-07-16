package com.netflix.priam;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.priam.defaultimpl.PriamConfiguration;

import java.util.Map;
import java.util.Properties;

/**
 * Loads {@link System#getProperties()} as a source.
 * <p/>
 * Implementation note: {@link #set(String, String)} does not write to system properties, but will write to a new map.
 * This means that setting values to this source has no effect on system properties or other instances of this class.
 */
public final class SystemPropertiesConfigSource extends AbstractConfigSource 
{
    private static final String BLANK = "";

    private final Map<String, String> data = Maps.newConcurrentMap();

    @Override
    public void initialize(final String asgName, final String region)
    {
        super.initialize(asgName, region);

        Properties systemProps = System.getProperties();

        for (final String key : systemProps.stringPropertyNames()) 
        {
            if (!key.startsWith(PriamConfiguration.PRIAM_PRE))
                continue;
            final String value = systemProps.getProperty(key);
            if (value != null && !BLANK.equals(value)) 
            {
                data.put(key, value);
            }
        }
    }

    @Override
    public int size() 
    {
        return data.size();
    }

    @Override
    public String get(final String key) 
    {
        return data.get(key);
    }

    @Override
    public void set(final String key, final String value) 
    {
        Preconditions.checkNotNull(value, "Value can not be null for configurations.");
        data.put(key, value);
    }
}
