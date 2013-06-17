package com.netflix.priam;

import com.google.common.collect.Maps;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

public final class SystemPropertiesConfigSource extends AbstractConfigSource {
  private static final String PRIAM_PRE = "priam";
  private static final String BLANK = "";

  private final Map<String, String> data = Maps.newConcurrentMap();

  @Override
  public void intialize(final String asgName, final String region) {
    super.intialize(asgName, region);

    Properties systemProps = System.getProperties();

    for (Enumeration en = systemProps.propertyNames(); en.hasMoreElements();)
    {
      String key = (String) en.nextElement();

      if (!key.startsWith(PRIAM_PRE))
        continue;

      String value = (String) systemProps.getProperty(key);

      if (value != null && !BLANK.equals(value))
        set(key, systemProps.getProperty(key));
    }
  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public String get(final String key) {
    return data.get(key);
  }

  @Override
  public void set(final String key, final String value) {
    data.put(key, value);
  }
}
