package com.netflix.priam.dse;

import java.util.Set;

/**
 * Datastax Enterprise-specific properties.
 *
 * @author jason brown
 */
public interface IDseConfiguration
{
    /**
     * Using Datastax's terms here for the different types of nodes.
     */
    public enum NodeType
    {
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

        private NodeType(String altName)
        {
            this.altName = altName;
        }

        public static NodeType getByAltName(String altName)
        {
            for(NodeType nt : NodeType.values())
            {
                if(nt.altName.toLowerCase().equals(altName))
                    return nt;
            }
            throw new IllegalArgumentException("Unknown node type: " + altName);
        }
    };

    String getDseYamlLocation();

    String getDseDelegatingSnitch();

    NodeType getNodeType();

    /* audit log configuration */

    boolean isAuditLogEnabled();

    /** @return comma-delimited list of keyspace names  */
    String getAuditLogExemptKeyspaces();

    /**
     * DSE-defined audit logging categories
     * http://www.datastax.com/docs/datastax_enterprise3.1/security/data_auditing#data-auditing
     */
    public enum AuditLogCategory { ADMIN, ALL, AUTH, DML, DDL, DCL, QUERY };

    Set<AuditLogCategory> getAuditLogCategories();
}
