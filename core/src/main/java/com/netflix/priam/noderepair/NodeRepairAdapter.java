package com.netflix.priam.noderepair;

import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public final class NodeRepairAdapter implements Job {
    private final NodeRepair nodeRepair;

    public NodeRepairAdapter(CassandraConfiguration cassandraConfig) {
        this.nodeRepair = new NodeRepair(cassandraConfig);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
       nodeRepair.execute();
    }
}
