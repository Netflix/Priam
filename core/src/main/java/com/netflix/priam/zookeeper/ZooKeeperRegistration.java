package com.netflix.priam.zookeeper;

import com.bazaarvoice.soa.ServiceEndPoint;
import com.bazaarvoice.soa.ServiceEndPointBuilder;
import com.bazaarvoice.soa.ServiceRegistry;
import com.bazaarvoice.soa.registry.ZooKeeperServiceRegistry;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.base.Optional;
import com.google.common.io.Closeables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Monitors the Cassandra server and adds an entry for it in ZooKeeper in the place and format expected by the BV SOA
 * {@link com.bazaarvoice.soa.HostDiscovery} class.  Clients are encouraged to use these entries in ZooKeeper to get
 * their initial seed lists when connecting to Cassandra.
 * <p>
 * The host discovery entry is tied to the state of the Cassandra thrift interface.  If the thrift interface is
 * disabled (eg. via "nodetool disablethrift") but the Cassandra node is left running, the entry in ZooKeeper will be
 * removed.
 */
public class ZooKeeperRegistration implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperRegistration.class);

    private final CassandraConfiguration casConfiguration;
    private final AmazonConfiguration awsConfiguration;
    private final Optional<ZooKeeperConnection> zkConnection;
    private final ScheduledExecutorService executor;
    private ServiceEndPoint endPoint;
    private ServiceRegistry zkRegistry;
    private boolean registered;

    @Inject
    public ZooKeeperRegistration(CassandraConfiguration casConfiguration,
                                 AmazonConfiguration awsConfiguration,
                                 Optional<ZooKeeperConnection> zkConnection) {
        this.casConfiguration = casConfiguration;
        this.awsConfiguration = awsConfiguration;
        this.zkConnection = zkConnection;

        String nameFormat = "ZooKeeperRegistration-%d";
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
        executor = Executors.newScheduledThreadPool(1, threadFactory);
    }

    @Override
    public synchronized void start() {
        if (!zkConnection.isPresent()) {
            return;
        }

        // Construct an SOA end point for this server.  The ID is the "host:port" that clients should use to connect.
        HostAndPort host = HostAndPort.fromParts(awsConfiguration.getPrivateIP(), casConfiguration.getThriftPort());
        endPoint = new ServiceEndPointBuilder()
                .withServiceName(casConfiguration.getClusterName() + "-cassandra")
                .withId(host.toString())
                .build();
        logger.info("ZooKeeper end point: {}", endPoint);

        // Connect to ZooKeeper
        zkRegistry = new ZooKeeperServiceRegistry(zkConnection.get());

        // Ping Cassandra every few seconds and register/deregister Cassandra when the thrift API is available.
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    update();
                } catch (Throwable t) {
                    logger.error("Unable to update ZooKeeper registration: " + t);
                }
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    private synchronized void update() {
        boolean alive;
        try {
            alive = JMXNodeTool.instance(casConfiguration).isThriftServerRunning();
        } catch (Exception e) {
            logger.info("Unable to use JMX to determine Cassandra thrift server status.", e);
            alive = false;
        }
        if (alive) {
            if (!registered) {
                logger.info("Registering Cassandra end point with ZooKeeper: {}", endPoint);
                zkRegistry.register(endPoint);
                registered = true;
            }
        } else {
            if (registered) {
                logger.info("Unregistering Cassandra end point with ZooKeeper: {}", endPoint);
                zkRegistry.unregister(endPoint);
                registered = false;
            }
        }
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
        Closeables.closeQuietly(zkRegistry);
    }
}
