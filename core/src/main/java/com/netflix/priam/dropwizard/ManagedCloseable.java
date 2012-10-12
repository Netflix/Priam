package com.netflix.priam.dropwizard;

import com.google.common.io.Closeables;
import com.yammer.dropwizard.lifecycle.Managed;

import java.io.Closeable;

/**
 * Adapts the Dropwizard {@link Managed} interface for a {@link Closeable}.
 */
public class ManagedCloseable implements Managed {
    private final Closeable _closeable;

    public ManagedCloseable(Closeable closeable) {
        _closeable = closeable;
    }

    @Override
    public void start() throws Exception {
        // do nothing
    }

    @Override
    public void stop() throws Exception {
        Closeables.closeQuietly(_closeable);
    }
}
