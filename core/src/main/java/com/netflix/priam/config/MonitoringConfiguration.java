package com.netflix.priam.config;

import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.NotNull;

public class MonitoringConfiguration {

    @JsonProperty @NotNull
    private boolean defaultBadgerRegistrationState = true; // True == registered, False == not registered

    @JsonProperty @NotNull
    private String badgerServiceName;

    public boolean getDefaultBadgerRegistrationState() {
        return defaultBadgerRegistrationState;
    }

    public void setDefaultBadgerRegistrationState(Boolean defaultBadgerRegistrationState) {
        this.defaultBadgerRegistrationState = defaultBadgerRegistrationState;
    }

    public String getBadgerServiceName() {
        return badgerServiceName;
    }

    public void setBadgerServiceName(String badgerServiceName) {
        this.badgerServiceName = badgerServiceName;
    }

}
