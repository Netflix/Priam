package com.netflix.priam.backupv2;

import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;

import javax.inject.Inject;

public class ThrowerFactory {
    private final Registry registry;

    @Inject
    public ThrowerFactory(Registry registry) {
        this.registry = registry;
    }

    public Thrower create(Logger logger) {
        return new Thrower(logger, registry);
    }
}
