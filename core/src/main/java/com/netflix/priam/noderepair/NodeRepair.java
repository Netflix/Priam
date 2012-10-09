package com.netflix.priam.noderepair;

import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.NodeRepairConfiguration;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


public final class NodeRepair {
    private static final Logger logger = LoggerFactory.getLogger(NodeRepair.class);
    private final String repairKeyspace;

    private CassandraConfiguration cassandraConfig;

    public NodeRepair(String keyspace, CassandraConfiguration cassandraConfig){
        this.repairKeyspace = keyspace;
        this.cassandraConfig = cassandraConfig;
    }

    public void execute()  {
        try{
            new RetryableCallable<Void>() {
                public Void retriableCall() throws Exception {
                    JMXNodeTool nodetool = new JMXNodeTool(cassandraConfig){
                        @Override
                        public void repair(boolean isSequential) throws IOException, ExecutionException, InterruptedException {
                            forceTableRepair(repairKeyspace, isSequential, new String[0]);
                        }
                    };
                    nodetool.repair(true);
                    return null;
                }
            }.call();
        }catch (Exception e) {
            logger.error("%s", e.getMessage());
        }
    }
}

