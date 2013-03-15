package com.netflix.priam.agent;

import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnFamily;

/**
 * Config values for the priam agent
 */
public interface AgentConfiguration
{
    /**
     * Keyspace to use for storing/reading agent JSON data
     *
     * @return keyspace
     */
    public Keyspace                     getKeyspace();

    /**
     * Column family to use for storing/reading agent JSON data
     *
     * @return Column family
     */
    public ColumnFamily<String, String> getColumnFamily();

    /**
     * The TTL to pass to {@link ColumnMutation#putValue(Object, Serializer, Integer)} when
     * storing agent JSON data
     *
     * @return ttl
     */
    public int                          getCassandraTtl();

    /**
     * Max number of threads to allow for processes. i.e. the maximum number of concurrent
     * processes.
     *
     * @return max
     */
    public int                          getMaxProcessThreads();

    /**
     * The max number of completed processes to keep track of
     *
     * @return max
     */
    public int                          getMaxCompletedProcesses();

    /**
     * This VM's hostname. This is used as the column name when storing agent data
     *
     * @return hostname
     */
    public String                       getThisHostName();
}
