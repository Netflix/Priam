package com.netflix.priam.dropwizard;

import java.io.Closeable;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;

import static java.lang.String.format;

/**
 * Enforces that only one instance of Priam is running at a time on a particular server.  There are race conditions
 * that can lead to corrupted SimpleDB entries when multiple instances of Priam on one server start at the same time.
 */
public class JvmMutex implements Closeable {
    private final ServerSocket _socket;

    public JvmMutex(int jvmMutexPort) throws IOException {
        try {
            _socket = new ServerSocket(jvmMutexPort);
        } catch (BindException e) {
            throw new IllegalStateException(format(
                    "Unable to obtain the jvm mutex port %d.  The most likely cause is that another " +
                            "instance of priam is already running.", jvmMutexPort), e);
        }
    }

    @Override
    public void close() throws IOException {
        _socket.close();
    }
}
