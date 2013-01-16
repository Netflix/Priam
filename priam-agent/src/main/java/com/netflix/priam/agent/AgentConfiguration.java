package com.netflix.priam.agent;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;

public interface AgentConfiguration
{
    public Keyspace                     getKeyspace();

    public ColumnFamily<String, String> getColumnFamily();

    public int                          getCassandraTtl();

    public int                          getMaxProcessThreads();

    public String                       getThisHostName();
}
