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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Base implementations for most methods on {@link IConfigSource}. */
public abstract class AbstractConfigSource implements IConfigSource {

    private String asgName;
    private String region;

    @Override
    public void initialize(final String asgName, final String region) {
        this.asgName = checkNotNull(asgName, "ASG name is not defined");
        this.region = checkNotNull(region, "Region is not defined");
    }

    @Override
    public boolean contains(final String key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public String get(final String key, final String defaultValue) {
        final String value = get(key);
        return (value != null) ? value : defaultValue;
    }

    @Override
    public boolean get(final String key, final boolean defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Boolean.parseBoolean(value);
            } catch (Exception e) {
                // ignore and return default
            }
        }
        return defaultValue;
    }

    @Override
    public Class<?> get(final String key, final Class<?> defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Class.forName(value);
            } catch (ClassNotFoundException e) {
                // ignore and return default
            }
        }
        return defaultValue;
    }

    @Override
    public <T extends Enum<T>> T get(final String key, final T defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Enum.valueOf(defaultValue.getDeclaringClass(), value);
            } catch (Exception e) {
                // ignore and return default.
            }
        }
        return defaultValue;
    }

    @Override
    public int get(final String key, final int defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (Exception e) {
                // ignore and return default
            }
        }
        return defaultValue;
    }

    @Override
    public long get(final String key, final long defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (Exception e) {
                // return default.
            }
        }
        return defaultValue;
    }

    @Override
    public float get(final String key, final float defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Float.parseFloat(value);
            } catch (Exception e) {
                // ignore and return default;
            }
        }
        return defaultValue;
    }

    @Override
    public double get(final String key, final double defaultValue) {
        final String value = get(key);
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (Exception e) {
                // ignore and return default.
            }
        }
        return defaultValue;
    }

    @Override
    public List<String> getList(String prop) {
        return getList(prop, ImmutableList.of());
    }

    @Override
    public List<String> getList(String prop, List<String> defaultValue) {
        final String value = get(prop);
        if (value != null) {
            return getTrimmedStringList(value.split(","));
        }
        return defaultValue;
    }

    protected String getAsgName() {
        return asgName;
    }

    protected String getRegion() {
        return region;
    }

    private List<String> getTrimmedStringList(String[] strings) {
        List<String> list = Lists.newArrayList();
        for (String s : strings) {
            list.add(StringUtils.strip(s));
        }
        return list;
    }
}
