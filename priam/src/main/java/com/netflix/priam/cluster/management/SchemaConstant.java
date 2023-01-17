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

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/** Created by aagrawal on 3/6/18. */
class SchemaConstant {
    private static final String SYSTEM_KEYSPACE_NAME = "system";
    private static final String SCHEMA_KEYSPACE_NAME = "system_schema";
    private static final String TRACE_KEYSPACE_NAME = "system_traces";
    private static final String AUTH_KEYSPACE_NAME = "system_auth";
    private static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";
    private static final String DSE_SYSTEM = "dse_system";

    private static final Set<String> SYSTEM_KEYSPACE_NAMES =
            ImmutableSet.of(
                    SYSTEM_KEYSPACE_NAME,
                    SCHEMA_KEYSPACE_NAME,
                    TRACE_KEYSPACE_NAME,
                    AUTH_KEYSPACE_NAME,
                    DISTRIBUTED_KEYSPACE_NAME,
                    DSE_SYSTEM);

    public static final boolean isSystemKeyspace(String keyspace) {
        return SYSTEM_KEYSPACE_NAMES.contains(keyspace.toLowerCase());
    }
}
