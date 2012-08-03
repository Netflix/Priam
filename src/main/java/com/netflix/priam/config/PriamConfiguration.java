package com.netflix.priam.config;

import com.google.inject.Singleton;
import com.yammer.dropwizard.config.Configuration;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Singleton
public class PriamConfiguration extends Configuration
{
    @JsonProperty @NotNull @Valid
    private CassandraConfiguration cassandra;

    @JsonProperty @NotNull @Valid
    private AmazonConfiguration amazon;

    @JsonProperty @NotNull @Valid
    private BackupConfiguration backup;

    public CassandraConfiguration getCassandraConfiguration()
    {
        return cassandra;
    }

    public AmazonConfiguration getAmazonConfiguration()
    {
        return amazon;
    }

    public BackupConfiguration getBackupConfiguration()
    {
        return backup;
    }
}
