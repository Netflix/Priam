package com.netflix.priam.noderepair;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class NodeRepair {
    private static final Logger logger = LoggerFactory.getLogger(NodeRepair.class);
    private static Duration lockAcquireTimeOut = Duration.standardMinutes(10);  //default time out
    private final Object lock = new Object();

    private CassandraConfiguration cassandraConfig;
    private Optional<ZooKeeperConnection> zooKeeperConnection;
    private AmazonConfiguration amazonConfiguration;

    public NodeRepair(CassandraConfiguration cassandraConfig, Optional<ZooKeeperConnection> zooKeeperConnection, AmazonConfiguration amazonConfiguration){
        this.cassandraConfig = cassandraConfig;
        this.zooKeeperConnection = zooKeeperConnection;
        this.amazonConfiguration = amazonConfiguration;
        this.lockAcquireTimeOut = Duration.standardMinutes(cassandraConfig.getLockAcquireTimeOut());
    }

    public void execute()  {
        try {
            if (!zooKeeperConnection.isPresent()) {
                return;
            }
            JMXNodeTool jmxNodeTool = getJMXNodeTool();
            jmxNodeTool.repair(true);
            logger.info("successfully finished node repair");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JMXNodeTool getJMXNodeTool() throws Exception {
        return new JMXNodeTool(cassandraConfig) {
            @Override
            public void repair(boolean isSequential) throws IOException, ExecutionException, InterruptedException {
                logger.info("started node repairing");
                Queue<String> keyspaceQueue = new LinkedList<String>();
                keyspaceQueue.addAll(getKeyspaces());
                logger.info("{} keyspaces are yet to repair", keyspaceQueue.size());

                while (keyspaceQueue.size() > 0) {
                    String keyspace = keyspaceQueue.remove();
                    InterProcessMutex mutex = provideInMutex(zooKeeperConnection.get().withNamespace("/applications/priam/noderepair"), getMutexName(keyspace));
                    try {
                        logger.info("node repair is trying to get lock of keyspace {}, thread: {}", keyspace, Thread.currentThread().getId());
                        if (mutex.acquire(lockAcquireTimeOut.getStandardMinutes(), TimeUnit.MINUTES)) {
                            try {
                                logger.info("starting node repair of keyspace {}, thread: {}", keyspace, Thread.currentThread().getId());
                                forceTableRepair(keyspace, isSequential, new String[0]);
                            } finally {
                                mutex.release();
                                logger.info("node repair of keyspace {} is done..lock released, thread: {}", keyspace, Thread.currentThread().getId());
                            }
                        } else {
                            logger.info("time out occurred acquiring lock for keyspace {}, thread: {}", keyspace, Thread.currentThread().getId());
                            mutex = null;
                            keyspaceQueue.add(keyspace);
                            Thread.sleep(2000);
                        }
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        };
    }

    private InterProcessMutex provideInMutex(ZooKeeperConnection connection, String mutexName){
        CuratorFramework curator = ((CuratorConnection) connection).getCurator();
        final InterProcessMutex mutex = new InterProcessMutex(curator, "/" + mutexName);
        return mutex;
    }

    private String getMutexName(String keyspace){
        return amazonConfiguration.getRegionName()+"/"+cassandraConfig.getClusterName()+"/"+keyspace;
    }

}

