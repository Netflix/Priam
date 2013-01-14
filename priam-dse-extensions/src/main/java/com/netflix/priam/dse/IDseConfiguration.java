package com.netflix.priam.dse;

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
        /** Vanilla Cassandra node */
        REAL_TIME_QUERY,
        /** Hadoop node */
        ANALYTIC,
        /** Solr node */
        SEARCH
    };

    String getDseYamlLocation();

    String getDseDelegatingSnitch();

    NodeType getNodeType();
}
