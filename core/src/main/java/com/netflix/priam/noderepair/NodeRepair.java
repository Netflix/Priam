package com.netflix.priam.noderepair;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.internal.CuratorConnection;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessLock;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.Mutex;
import com.netflix.priam.utils.RetryableCallable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Singleton
public final class NodeRepair {
    private static final Logger logger = LoggerFactory.getLogger(NodeRepair.class);
    private static final Duration LOCK_ACQUIRE_TIMEOUT = Duration.standardMinutes(1);

    private CassandraConfiguration cassandraConfig;
    private ZooKeeperConnection zooKeeperConnection;
    private AmazonConfiguration amazonConfiguration;

    public NodeRepair(CassandraConfiguration cassandraConfig, ZooKeeperConnection zooKeeperConnection, AmazonConfiguration amazonConfiguration){
        this.cassandraConfig = cassandraConfig;
        this.zooKeeperConnection = zooKeeperConnection;
        this.amazonConfiguration = amazonConfiguration;
    }

    public void execute()  {
        try {
            JMXNodeTool jmxNodeTool = getJMXNodeTool();
            jmxNodeTool.repair(true);
        } catch (Exception e) {

        }
    }

    private JMXNodeTool getJMXNodeTool() throws Exception {
        return new JMXNodeTool(cassandraConfig){
            @Override
            public void repair(boolean isSequential) throws IOException, ExecutionException, InterruptedException {
                Queue keyspaceQueue = new LinkedList<String>();
                keyspaceQueue.add(getKeyspaces());
                while (keyspaceQueue.size() > 0) {
                    String keyspace = (String) keyspaceQueue.remove();
                    //try to get Mute
                    InterProcessMutex mutex = provideInMutex(zooKeeperConnection.withNamespace("/applications/priam/noderepair"), getMutexName(keyspace));
                    try {
                        // try to acquire mutex for index within flush period
                        logger.info("for node repair trying to get lock of keyspace {}", keyspace);
                        if (mutex.acquire(LOCK_ACQUIRE_TIMEOUT.getStandardMinutes(), TimeUnit.MINUTES)) {
                            try {
                                logger.info("starting node repair of keyspace {}", keyspace);
                                forceTableRepair(keyspace, isSequential, new String[0]);
                            } finally {
                                mutex.release();
                                logger.info("node repair of keyspace {} is done,releasing lock", keyspace);
                            }
                        } else {
                            logger.info("could not acquire index lock after {} minutes!!", LOCK_ACQUIRE_TIMEOUT);
                            keyspaceQueue.add(keyspace);
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

