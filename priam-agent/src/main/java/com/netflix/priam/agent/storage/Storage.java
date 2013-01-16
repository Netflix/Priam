package com.netflix.priam.agent.storage;

import com.netflix.priam.agent.AgentConfiguration;

public interface Storage
{
    public String       getValue(AgentConfiguration configuration, String rowKey, String columnName) throws Exception;

    public void         setValue(AgentConfiguration configuration, String rowKey, String columnName, String value) throws Exception;
}
