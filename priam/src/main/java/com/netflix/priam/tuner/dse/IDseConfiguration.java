/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.tuner.dse;

import java.util.Set;

/**
 * Datastax Enterprise-specific properties.
 *
 * @author jason brown
 */
public interface IDseConfiguration {
    /** Using Datastax's terms here for the different types of nodes. */
    enum NodeType {
        /** vanilla Cassandra node */
        REAL_TIME_QUERY("cassandra"),

        /** Hadoop node */
        ANALYTIC_HADOOP("hadoop"),

        /** Spark node */
        ANALYTIC_SPARK("spark"),

        /** Hadoop and Spark node */
        ANALYTIC_HADOOP_SPARK("hadoop-spark"),

        /** Solr node */
        SEARCH("solr");

        private final String altName;

        NodeType(String altName) {
            this.altName = altName;
        }

        public static NodeType getByAltName(String altName) {
            for (NodeType nt : NodeType.values()) {
                if (nt.altName.toLowerCase().equals(altName)) return nt;
            }
            throw new IllegalArgumentException("Unknown node type: " + altName);
        }
    }

    String getDseYamlLocation();

    String getDseDelegatingSnitch();

    NodeType getNodeType();

    /* audit log configuration */

    boolean isAuditLogEnabled();

    /** @return comma-delimited list of keyspace names */
    String getAuditLogExemptKeyspaces();

    /**
     * DSE-defined audit logging categories
     * http://www.datastax.com/docs/datastax_enterprise3.1/security/data_auditing#data-auditing
     */
    enum AuditLogCategory {
        ADMIN,
        ALL,
        AUTH,
        DML,
        DDL,
        DCL,
        QUERY
    }

    Set<AuditLogCategory> getAuditLogCategories();
}
