package com.netflix.priam.config;

import com.google.inject.Singleton;
import com.yammer.dropwizard.config.Configuration;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Singleton
public class PriamConfiguration extends Configuration {
    @JsonProperty
    @NotNull
    @Valid
    private CassandraConfiguration cassandra = new CassandraConfiguration();

    @JsonProperty
    @Valid
    private AmazonConfiguration amazon = new AmazonConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private BackupConfiguration backup = new BackupConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private NodeRepairConfiguration nodeRepair = new NodeRepairConfiguration();
    private ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

    public CassandraConfiguration getCassandraConfiguration() {
        return cassandra;
    }

    public AmazonConfiguration getAmazonConfiguration() {
        return amazon;
    }

    public BackupConfiguration getBackupConfiguration() {
        return backup;
    }

    public NodeRepairConfiguration getNodeRepairConfiguration() {
        return nodeRepair;
    }

    public ZooKeeperConfiguration getZooKeeperConfiguration() {
        return zooKeeper;
    }
}
