package com.netflix.priam;

import com.google.inject.ImplementedBy;
import com.netflix.priam.defaultimpl.CassandraProcessManager;

import java.io.IOException;

/**
 * Interface to aid in starting and stopping cassandra.
 *
 * @author jason brown
 */
@ImplementedBy(CassandraProcessManager.class)
public interface ICassandraProcess
{
    void start(boolean join_ring) throws IOException;

    void stop() throws IOException;
}
