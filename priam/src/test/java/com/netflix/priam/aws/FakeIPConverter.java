package com.netflix.priam.aws;

import com.netflix.priam.identity.PriamInstance;
import groovy.lang.Singleton;
import java.util.Optional;

@Singleton
public class FakeIPConverter implements IPConverter {
    private boolean shouldFail;

    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    @Override
    public Optional<String> getPublicIP(PriamInstance instance) {
        return shouldFail ? Optional.empty() : Optional.of("1.1.1.1");
    }
}
