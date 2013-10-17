package com.netflix.priam.dropwizard.managers;

import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.bazaarvoice.ostrich.registry.zookeeper.ZooKeeperServiceRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.yammer.dropwizard.config.HttpConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Monitors the Cassandra server and adds an entry for it in ZooKeeper in the place and format expected by the BV SOA
 * {@link com.bazaarvoice.ostrich.HostDiscovery} class.  Clients are encouraged to use these entries in ZooKeeper to get
 * their initial seed lists when connecting to Cassandra.
 * <p>
 * The host discovery entry is tied to the state of the Cassandra thrift interface.  If the thrift interface is
 * disabled (eg. via "nodetool disablethrift") but the Cassandra node is left running, the entry in ZooKeeper will be
 * removed.
 */
@Singleton
public class ServiceRegistryManager implements Managed {
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistryManager.class);

    private final PriamConfiguration priamConfiguration;
    private final CassandraConfiguration casConfiguration;
    private final AmazonConfiguration awsConfiguration;
    private final Optional<CuratorFramework> zkConnection;
    private final ScheduledExecutorService executor;
    private final List<ServiceEndPoint> endPoints = Lists.newArrayList();
    private ServiceRegistry zkRegistry;
    private HttpConfiguration httpConfiguration;
    private boolean registered;

    @Inject
    public ServiceRegistryManager(PriamConfiguration priamConfiguration,
                                  CassandraConfiguration casConfiguration,
                                  AmazonConfiguration awsConfiguration,
                                  Optional<CuratorFramework> zkConnection,
                                  HttpConfiguration httpConfiguration) {
        this.priamConfiguration = priamConfiguration;
        this.casConfiguration = casConfiguration;
        this.awsConfiguration = awsConfiguration;
        this.zkConnection = zkConnection;
        this.httpConfiguration = httpConfiguration;

        String nameFormat = "ServiceRegistryManager-%d";
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
        executor = Executors.newScheduledThreadPool(1, threadFactory);
    }

    @Override
    public synchronized void start() throws Exception {
        if (!zkConnection.isPresent()) {
            return;
        }

        HostAndPort host = HostAndPort.fromParts(awsConfiguration.getPrivateIP(), casConfiguration.getThriftPort());
        String priamServiceBaseURL = String.format("http://%s:%s/%s", awsConfiguration.getPrivateIP(),
                httpConfiguration.getPort(), "v1");

        // Include the partitioner in the Ostrich end point data to support clients that need to know the
        // partitioner type before they connect to the ring (eg. Astyanax).
        Map<String, Object> payload = ImmutableMap.<String, Object>of(
                "partitioner", FBUtilities.newPartitioner(casConfiguration.getPartitionerClassName()).getClass().getName(),
                "url", priamServiceBaseURL);
        String payloadString = new ObjectMapper().writeValueAsString(payload);

        // Construct Ostrich end points for this server.  The ID is the "host:port" that clients should use to connect.
        for (String serviceName : priamConfiguration.getOstrichServiceNames()) {
            ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                    .withServiceName(serviceName)
                    .withId(host.toString())
                    .withPayload(payloadString)
                    .build();
            endPoints.add(endPoint);
            logger.info("ZooKeeper end point: {}", endPoint);
        }

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
                for (ServiceEndPoint endPoint : endPoints) {
                    logger.info("Registering Cassandra end point with ZooKeeper: {}", endPoint);
                    zkRegistry.register(endPoint);
                }
                registered = true;
            }
        } else {
            if (registered) {
                for (ServiceEndPoint endPoint : endPoints) {
                    logger.info("Unregistering Cassandra end point with ZooKeeper: {}", endPoint);
                    zkRegistry.unregister(endPoint);
                }
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
