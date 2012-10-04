package com.netflix.priam.config;

import org.codehaus.jackson.annotate.JsonProperty;

public class ZooKeeperConfiguration extends com.bazaarvoice.zookeeper.dropwizard.ZooKeeperConfiguration {

    @JsonProperty
    private boolean enabled;

    public boolean isEnabled() {
        return enabled;
    }
}
