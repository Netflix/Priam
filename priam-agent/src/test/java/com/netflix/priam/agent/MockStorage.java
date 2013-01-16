package com.netflix.priam.agent;

import com.google.common.collect.Maps;
import com.netflix.priam.agent.storage.Storage;
import java.util.Map;

public class MockStorage implements Storage
{
    private final Map<String, String>       data = Maps.newConcurrentMap();

    @Override
    public String getValue(AgentConfiguration configuration, String rowKey, String columnName) throws Exception
    {
        return data.get(makeKey(rowKey, columnName));
    }

    @Override
    public void setValue(AgentConfiguration configuration, String rowKey, String columnName, String value) throws Exception
    {
        data.put(makeKey(rowKey, columnName), value);
    }

    public Map<String, String> getData()
    {
        return data;
    }

    private String makeKey(String rowKey, String columnName)
    {
        return rowKey + "\t" + columnName;
    }
}
