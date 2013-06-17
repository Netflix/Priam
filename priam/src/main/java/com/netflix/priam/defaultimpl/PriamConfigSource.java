package com.netflix.priam.defaultimpl;

import com.google.common.collect.ImmutableList;
import com.netflix.priam.CompositeConfigSource;
import com.netflix.priam.ConfigSource;
import com.netflix.priam.ICredential;
import com.netflix.priam.PropertiesConfigSource;
import com.netflix.priam.SimpleDBConfigSource;
import com.netflix.priam.SystemPropertiesConfigSource;

import javax.inject.Inject;

/**
 * Default {@link ConfigSource} pulling in configs from SimpleDB, local Properties, and System Properties.
 */
public class PriamConfigSource extends CompositeConfigSource {

  @Inject
  public PriamConfigSource(final ICredential provider) {
    // this order was based off PriamConfigurations loading.  W/e loaded last could override, but with Composite, first
    // has the highest priority.
    super(ImmutableList.of(
        new SimpleDBConfigSource(provider),
        new PropertiesConfigSource(),
        new SystemPropertiesConfigSource()
    ));
  }
}
