package com.netflix.priam.noderepair;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.base.Optional;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public final class NodeRepairAdapter implements Job {
    private final NodeRepair nodeRepair;

    public NodeRepairAdapter(CassandraConfiguration cassandraConfig, Optional<ZooKeeperConnection> zooKeeperConnection, AmazonConfiguration amazonConfiguration) {
        this.nodeRepair = new NodeRepair(cassandraConfig, zooKeeperConnection, amazonConfiguration);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
       nodeRepair.execute();
    }
}
