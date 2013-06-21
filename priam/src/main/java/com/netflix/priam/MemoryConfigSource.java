package com.netflix.priam;

import com.google.common.collect.Maps;

import java.util.Map;

public final class MemoryConfigSource extends AbstractConfigSource {
  private final Map<String, String> data = Maps.newConcurrentMap();

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