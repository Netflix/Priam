/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
