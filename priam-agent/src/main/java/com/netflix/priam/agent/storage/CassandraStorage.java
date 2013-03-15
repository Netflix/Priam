package com.netflix.priam.agent.storage;

import com.netflix.astyanax.model.Column;
import com.netflix.priam.agent.AgentConfiguration;

/**
 * Cassandra version of storage
 */
public class CassandraStorage implements Storage
{
    @Override
    public String getValue(AgentConfiguration configuration, String rowKey, String columnName) throws Exception
    {
        Column<String> result = configuration.getKeyspace()
            .prepareQuery(configuration.getColumnFamily())
            .getKey(rowKey)
            .getColumn(configuration.getThisHostName())
            .execute()
            .getResult();
        return result.hasValue() ? result.getStringValue() : null;
    }

    @Override
    public void setValue(AgentConfiguration configuration, String rowKey, String columnName, String value) throws Exception
    {
        configuration.getKeyspace()
            .prepareColumnMutation(configuration.getColumnFamily(), rowKey, configuration.getThisHostName())
            .putValue(value, configuration.getCassandraTtl())
            .execute();
    }
}
