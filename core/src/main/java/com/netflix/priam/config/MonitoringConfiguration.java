package com.netflix.priam.config;

import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.NotNull;

public class MonitoringConfiguration {

    @JsonProperty @NotNull
    private Boolean defaultBadgerRegistrationState = Boolean.TRUE; // True == registered, False == not registered

    @JsonProperty @NotNull
    private String badgerServiceName;

    public Boolean getDefaultBadgerRegistrationState() {
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
