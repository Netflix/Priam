package com.netflix.priam.agent.storage;

import com.netflix.priam.agent.AgentConfiguration;

/**
 * Interface for storing agent data
 */
public interface Storage
{
    /**
     * Return the data for the given row/column
     *
     * @param configuration config
     * @param rowKey row
     * @param columnName column
     * @return data or null if it doesn't exist
     * @throws Exception errors
     */
    public String       getValue(AgentConfiguration configuration, String rowKey, String columnName) throws Exception;

    /**
     * Set the value for the given row/column
     *
     * @param configuration config
     * @param rowKey row
     * @param columnName column
     * @param value value
     * @throws Exception errors
     */
    public void         setValue(AgentConfiguration configuration, String rowKey, String columnName, String value) throws Exception;
}
