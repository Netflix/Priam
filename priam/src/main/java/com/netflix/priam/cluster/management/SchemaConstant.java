package com.netflix.priam.cluster.management;

/**
 * Created by aagrawal on 3/6/18.
 */
public class SchemaConstant {
    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String SCHEMA_KEYSPACE_NAME = "system_schema";
    public static final String TRACE_KEYSPACE_NAME = "system_traces";
    public static final String AUTH_KEYSPACE_NAME = "system_auth";
    public static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";
    public static final String DSE_SYSTEM = "dse_system";

    public static final boolean shouldAvoidKeyspaceForClusterMgmt(String keyspace){
        if (keyspace.equalsIgnoreCase(SYSTEM_KEYSPACE_NAME) ||
                keyspace.equalsIgnoreCase(SCHEMA_KEYSPACE_NAME) ||
                keyspace.equalsIgnoreCase(TRACE_KEYSPACE_NAME) ||
                keyspace.equalsIgnoreCase(AUTH_KEYSPACE_NAME) ||
                keyspace.equalsIgnoreCase(DISTRIBUTED_KEYSPACE_NAME) ||
                keyspace.equalsIgnoreCase(DSE_SYSTEM))
            return true;

        return false;
    }
}
