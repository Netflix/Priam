package com.netflix.priam;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Loads the 'Priam.properties' file as a source.
 */
public class PropertiesConfigSource extends AbstractConfigSource {
  private static final Logger logger = LoggerFactory.getLogger(PropertiesConfigSource.class.getName());

  private final Map<String, String> data = Maps.newConcurrentMap();

  public PropertiesConfigSource() {
  }

  public PropertiesConfigSource(final Properties properties) {
    checkNotNull(properties);
    clone(properties);
  }

  @Override
  public void intialize(final String asgName, final String region) {
    super.intialize(asgName, region);
    ClassLoader loader = PropertiesConfigSource.class.getClassLoader();
    Properties properties = new Properties();
    URL url = loader.getResource("Priam.properties");
    if (url != null) {
      try {
        properties.load(url.openStream());
        clone(properties);
      } catch (IOException e) {
        logger.info("No Priam.properties. Ignore!");
      }
    } else {
      logger.info("No Priam.properties. Ignore!");
    }
  }

  @Override
  public String get(final String prop) {
    return data.get(prop);
  }

  @Override
  public void set(final String key, final String value) {
    Preconditions.checkNotNull(value, "Value can not be null for configurations.");
    data.put(key, value);
  }


  @Override
  public int size() {
    return data.size();
  }

  @Override
  public boolean contains(final String prop) {
    return data.containsKey(prop);
  }

  /**
   * Clones all the values from the properties.  If the value is null, it will be ignored.
   *
   * @param properties to clone
   */
  private void clone(final Properties properties) {
    if (properties.isEmpty()) return;

    synchronized (properties) {
      for (final String key : properties.stringPropertyNames()) {
        final String value = properties.getProperty(key);
        if (value != null) {
          data.put(key, value);
        }
      }
    }
  }
}
