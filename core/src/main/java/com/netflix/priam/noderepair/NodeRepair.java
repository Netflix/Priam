package com.netflix.priam.noderepair;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.JMXNodeTool;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public final class NodeRepair extends Task {
    public static String JOBNAME = "NodeRepair";
    private static final Logger logger = LoggerFactory.getLogger(NodeRepair.class);
    private static Duration nodeRepairMutexAcquireTimeOut = Duration.standardMinutes(10);  //default time out

    private CassandraConfiguration cassandraConfig;
    private Optional<ZooKeeperConnection> zooKeeperConnection;
    private AmazonConfiguration amazonConfiguration;

    @Inject
    public NodeRepair(CassandraConfiguration cassandraConfig, Optional<ZooKeeperConnection> zooKeeperConnection, AmazonConfiguration amazonConfiguration){
        this.cassandraConfig = cassandraConfig;
        this.zooKeeperConnection = zooKeeperConnection;
        this.amazonConfiguration = amazonConfiguration;
        this.nodeRepairMutexAcquireTimeOut = Duration.standardMinutes(cassandraConfig.getNodeRepairMutexAcquireTimeOut());
    }

    public void execute()  {
        try {
            if (!zooKeeperConnection.isPresent()) {
                return;
            }
            JMXNodeTool jmxNodeTool = new JMXNodeTool(cassandraConfig);

            logger.info("started node repairing");
            Queue<String> keyspaceQueue = new LinkedList<String>();
            keyspaceQueue.addAll(jmxNodeTool.getKeyspaces());
            logger.info("{} keyspaces are yet to repair", keyspaceQueue.size());

            //while there are unrepaired keyspaces
            while (keyspaceQueue.size() > 0) {
                String keyspace = keyspaceQueue.remove();
                //get mutex for the keyspace
                InterProcessMutex mutex = provideInMutex(zooKeeperConnection.get().withNamespace("/applications/priam/noderepair"), getMutexName(keyspace));
                try {
                    logger.info("node repair is trying to get lock of keyspace {}, thread: {}", keyspace, Thread.currentThread().getId());
                    if (mutex.acquire(nodeRepairMutexAcquireTimeOut.getStandardMinutes(), TimeUnit.MINUTES)) {
                        logger.info("starting node repair of keyspace {}, thread: {}", keyspace, Thread.currentThread().getId());
                        jmxNodeTool.repair(keyspace,true);
                        logger.info("node repair of keyspace {} is done, thread: {}", keyspace, Thread.currentThread().getId());
                    } else {
                        logger.info("time out occurred acquiring lock for keyspace {}, thread: {}", keyspace, Thread.currentThread().getId());
                        //add the keyspace back to the Queue
                        keyspaceQueue.add(keyspace);
                        Thread.sleep(2000);
                    }
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                } finally {
                    //release the mutex regardless of mutex was acquired or not
                    try { mutex.release(); } catch (Throwable e) { /* don't care */ };
                }
            }
            logger.info("successfully finished node repair");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private InterProcessMutex provideInMutex(ZooKeeperConnection connection, String mutexName){
        CuratorFramework curator = ((CuratorConnection) connection).getCurator();
        final InterProcessMutex mutex = new InterProcessMutex(curator, "/" + mutexName);
        return mutex;
    }

    private String getMutexName(String keyspace){
        return amazonConfiguration.getRegionName()+"/"+cassandraConfig.getClusterName()+"/"+keyspace;
    }

    @Override
    public String getCronTime(){
        return cassandraConfig.getNodeRepairTime();
    }

    public String getTriggerName() {
        return "noderepair-trigger";
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

}

