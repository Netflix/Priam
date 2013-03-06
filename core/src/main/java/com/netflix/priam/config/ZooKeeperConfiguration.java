package com.netflix.priam.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ZooKeeperConfiguration extends com.bazaarvoice.curator.dropwizard.ZooKeeperConfiguration {

    @JsonProperty
    private boolean enabled;

    public boolean isEnabled() {
        return enabled;
    }
}
