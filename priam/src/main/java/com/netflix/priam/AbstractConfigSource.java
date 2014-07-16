package com.netflix.priam;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base implementations for most methods on {@link IConfigSource}.
 */
public abstract class AbstractConfigSource implements IConfigSource 
{

    private String asgName;
    private String region;

    @Override
    public void initialize(final String asgName, final String region)
    {
        this.asgName = checkNotNull(asgName, "ASG name is not defined");
        this.region = checkNotNull(region, "Region is not defined");
    }

    @Override
    public boolean contains(final String key) 
    {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() 
    {
        return size() == 0;
    }

    @Override
    public String get(final String key, final String defaultValue) 
    {
        final String value = get(key);
        return (value != null) ? value : defaultValue;
    }

    @Override
    public boolean get(final String key, final boolean defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Boolean.parseBoolean(value);
            } 
            catch (Exception e) {
              // ignore and return default
            }
        }
        return defaultValue;
    }

    @Override
    public Class<?> get(final String key, final Class<?> defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Class.forName(value);
            } 
            catch (ClassNotFoundException e) 
            {
              // ignore and return default
            }
        }
        return defaultValue;
    }

    @Override
    public <T extends Enum<T>> T get(final String key, final T defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Enum.valueOf(defaultValue.getDeclaringClass(), value);
            } 
            catch (Exception e) 
            {
              // ignore and return default.
            }
        }
        return defaultValue;
    }

    @Override
    public int get(final String key, final int defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Integer.parseInt(value);
            } 
            catch (Exception e) 
            {
              // ignore and return default
            }
        }
        return defaultValue;
    }

    @Override
    public long get(final String key, final long defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Long.parseLong(value);
            } 
            catch (Exception e) 
            {
              // return default.
            }
        }
        return defaultValue;
    }

    @Override
    public float get(final String key, final float defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Float.parseFloat(value);
            } 
            catch (Exception e) 
            {
              // ignore and return default.
            }
        }
        return defaultValue;
    }

    @Override
    public double get(final String key, final double defaultValue) 
    {
        final String value = get(key);
        if (value != null) 
        {
            try 
            {
                return Double.parseDouble(value);
            } 
            catch (Exception e) 
            {
              // ignore and return default.
            }
        }
        return defaultValue;
    }

    @Override
    public List<String> getList(String prop) 
    {
          return getList(prop, ImmutableList.<String>of());
    }

    @Override
    public List<String> getList(String prop, List<String> defaultValue) 
    {
        final String value = get(prop);
        if (value != null) 
        {
            return getTrimmedStringList(value.split(","));
        }
        return defaultValue;
    }

    protected String getAsgName() 
    {
        return asgName;
    }

    protected String getRegion() 
    {
        return region;
    }

    private List<String> getTrimmedStringList(String[] strings) 
    {
        List<String> list = Lists.newArrayList();
        for (String s : strings) 
        {
            list.add(StringUtils.strip(s));
        }
        return list;
    }

}
