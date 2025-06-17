package com.netflix.priam.backupv2;

import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

public class Thrower {
    private final Logger logger;
    private final ImmutableMap<Failure, Counter> metrics;

    public Thrower(Logger logger, Registry registry) {
        this.logger = logger;
        ImmutableMap.Builder<Failure, Counter> builder = ImmutableMap.builder();
        for (Failure failure : Failure.values()) {
            builder.put(failure, registry.counter(failure.toString()));
        }
        metrics = builder.build();
    }

    void abort(@Nonnull Failure failure, String message) throws BackupDeletionException {
        log(failure, message);
        throw new BackupDeletionException(message);
    }

    void abort(@Nonnull Failure failure, String message, Exception e) throws BackupDeletionException {
        log(failure, message);
        throw new BackupDeletionException(message, e);
    }

    void log(@Nonnull Failure failure, String message) {
        metrics.get(failure).increment();
        logger.warn("{}, {}", failure.name(), message);
    }
}
