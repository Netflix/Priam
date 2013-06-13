package com.netflix.priam;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class PropertiesConfigSource extends AbstractConfigSource {

  private final Properties properties;

  public PropertiesConfigSource() {
    this.properties = new Properties();
  }

  public PropertiesConfigSource(final Properties properties) {
    this.properties = checkNotNull(properties);
  }

  @Override
  public String get(final String prop) {
    return properties.getProperty(prop);
  }


  @Override
  public void set(final String key, final String value) {
    properties.put(key, value);
  }

  @Override
  public int size() {
    return properties.size();
  }

  @Override
  public boolean contains(final String prop) {
    return properties.containsKey(prop);
  }
}
