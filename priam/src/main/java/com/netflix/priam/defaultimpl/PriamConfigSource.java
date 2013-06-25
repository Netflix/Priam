package com.netflix.priam.defaultimpl;

import com.netflix.priam.CompositeConfigSource;
import com.netflix.priam.PropertiesConfigSource;
import com.netflix.priam.SimpleDBConfigSource;
import com.netflix.priam.SystemPropertiesConfigSource;

import javax.inject.Inject;

/**
 * Default {@link com.netflix.priam.IConfigSource} pulling in configs from SimpleDB, local Properties, and System Properties.
 */
public class PriamConfigSource extends CompositeConfigSource {

  @Inject
  public PriamConfigSource(final SimpleDBConfigSource simpleDBConfigSource,
                           final PropertiesConfigSource propertiesConfigSource,
                           final SystemPropertiesConfigSource systemPropertiesConfigSource) {
    // this order was based off PriamConfigurations loading.  W/e loaded last could override, but with Composite, first
    // has the highest priority.
    super(simpleDBConfigSource,
        propertiesConfigSource,
        systemPropertiesConfigSource);
  }
}
