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
        /** vanilla Cassandra node */
        REAL_TIME_QUERY("cassandra"),
        /** Hadoop node */
        ANALYTIC("hadoop"),
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
}
