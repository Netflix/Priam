package com.netflix.priam;

import java.io.IOException;

/**
 * Interface to aid in starting and stopping cassandra.
 *
 * @author jason brown
 */
public interface ICassandraProcess
{
    void start(boolean join_ring) throws IOException;

    void stop() throws IOException;
}
