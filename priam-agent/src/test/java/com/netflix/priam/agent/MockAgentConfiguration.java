package com.netflix.priam.agent;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;

public class MockAgentConfiguration implements AgentConfiguration
{
    private final String thisHostname;

    public MockAgentConfiguration(String thisHostname)
    {
        this.thisHostname = thisHostname;
    }

    @Override
    public Keyspace getKeyspace()
    {
        return null;    // not used
    }

    @Override
    public ColumnFamily<String, String> getColumnFamily()
    {
        return null;    // not used
    }

    @Override
    public int getCassandraTtl()
    {
        return 0;
    }

    @Override
    public int getMaxProcessThreads()
    {
        return 10;
    }

    @Override
    public int getMaxCompletedProcesses()
    {
        return 100;
    }

    @Override
    public String getThisHostName()
    {
        return thisHostname;
    }
}
