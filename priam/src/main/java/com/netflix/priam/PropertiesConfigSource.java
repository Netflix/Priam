package com.netflix.priam;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class PropertiesConfigSource extends AbstractConfigSource {
  private static final Logger logger = LoggerFactory.getLogger(PropertiesConfigSource.class.getName());

  private final Properties properties;

  public PropertiesConfigSource() {
    this.properties = new Properties();
  }

  public PropertiesConfigSource(final Properties properties) {
    this.properties = checkNotNull(properties);
  }

  @Override
  public void intialize(final String asgName, final String region) {
    super.intialize(asgName, region);
    ClassLoader loader = PropertiesConfigSource.class.getClassLoader();
    URL url = loader.getResource("Priam.properties");
    if (url != null) {
      try {
        properties.load(url.openStream());
      } catch (IOException e) {
        logger.info("No Priam.properties. Ignore!");
      }
    } else {
      logger.info("No Priam.properties. Ignore!");
    }
  }

  @Override
  public String get(final String prop) {
    return properties.getProperty(prop);
  }


  @Override
  public void set(final String key, final String value) {
    Preconditions.checkNotNull(value, "Value can not be null for configurations.");
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
