package com.netflix.priam.dse;

/**
 * Datastax Enterprise-specific properties.
 *
 * @author jason brown
 */
public interface IDseConfiguration
{
    String getDseYamlLocation();

    String getDseDelegatingSnitch();
}
