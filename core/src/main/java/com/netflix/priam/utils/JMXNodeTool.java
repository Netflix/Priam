package com.netflix.priam.utils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;


/**
 * Class to get data out of Cassandra JMX
 */
@Singleton
public class JMXNodeTool extends NodeProbe {
    private static final Logger logger = LoggerFactory.getLogger(JMXNodeTool.class);
    private static volatile JMXNodeTool tool = null;
    private MBeanServerConnection mbeanServerConn = null;

    /**
     * Hostname and Port to talk to will be same server for now optionally we
     * might want the ip to poll.
     * <p/>
     * NOTE: This class shouldn't be a singleton and this shouldn't be cached.
     * <p/>
     * This will work only if cassandra runs.
     */
    public JMXNodeTool(String host, int port) throws IOException, InterruptedException {
        super(host, port);
    }

    @Inject
    public JMXNodeTool(CassandraConfiguration cassandraConfiguration) throws IOException, InterruptedException {
        super("localhost", cassandraConfiguration.getJmxPort());
    }

    /**
     * try to create if it is null.
     */
    public static JMXNodeTool instance(CassandraConfiguration config) throws Exception {
        if (!testConnection()) {
            reconnect(config);
        }
        return tool;
    }

    public static <T> T getRemoteBean(Class<T> clazz, String mbeanName, CassandraConfiguration config, boolean mxbean) {
        try {
            if (mxbean) {
                return ManagementFactory.newPlatformMXBeanProxy(JMXNodeTool.instance(config).mbeanServerConn, mbeanName, clazz);
            } else {
                return JMX.newMBeanProxy(JMXNodeTool.instance(config).mbeanServerConn, new ObjectName(mbeanName), clazz);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * This method will test if you can connect and query something before handing over the connection,
     * This is required for our retry logic.
     *
     * @return
     */
    private static boolean testConnection() {
        // connecting first time hence return false.
        if (tool == null) {
            return false;
        }

        try {
            tool.isInitialized();
        } catch (Throwable ex) {
            SystemUtils.closeQuietly(tool);
            return false;
        }
        return true;
    }

    private static synchronized void reconnect(CassandraConfiguration config) throws Exception {
        // Recheck connection in case we were beaten to the punch by another reconnect call.
        if (testConnection()) {
            return;
        }
        tool = connect(config);
    }

    public static synchronized JMXNodeTool connect(final CassandraConfiguration config) throws Exception {
        return new RetryableCallable<JMXNodeTool>(false) {
            @Override
            public JMXNodeTool retriableCall() throws Exception {
                JMXNodeTool nodetool = new JMXNodeTool("localhost", config.getJmxPort());
                Field fields[] = NodeProbe.class.getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    if (!fields[i].getName().equals("mbeanServerConn")) {
                        continue;
                    }
                    fields[i].setAccessible(true);
                    nodetool.mbeanServerConn = (MBeanServerConnection) fields[i].get(nodetool);
                }
                return nodetool;
            }
        }.call();
    }

    /**
     * You must do the compaction before running this to remove the duplicate
     * tokens out of the server. TODO code it.
     */
    @SuppressWarnings ("unchecked")
    public Map<String, Object> estimateKeys() {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = super.getColumnFamilyStoreMBeanProxies();
        Map<String, Object> object = Maps.newHashMap();
        while (it.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            object.put("keyspace", entry.getKey());
            object.put("column_family", entry.getValue().getColumnFamilyName());
            object.put("estimated_size", entry.getValue().estimateKeys());
        }
        return object;
    }

    @SuppressWarnings ("unchecked")
    public Map<String, Object> info() {
        logger.info("JMX info being called");
        Map<String, Object> object = Maps.newHashMap();
        object.put("gossip_active", isInitialized());
        object.put("thrift_active", isThriftServerRunning());
        object.put("token", getToken());
        object.put("load", getLoadString());
        object.put("generation_no", getCurrentGenerationNumber());
        object.put("uptime", getUptime() / 1000);
        MemoryUsage heapUsage = getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        object.put("heap_memory_mb", memUsed + "/" + memMax);
        object.put("data_center", getDataCenter());
        object.put("rack", getRack());
        logger.info(object.toString());
        return object;
    }

    @SuppressWarnings ("unchecked")
    public long totalHints()
            throws MalformedObjectNameException {
        ObjectName name = new ObjectName(StorageProxy.MBEAN_NAME);
        StorageProxyMBean storageProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageProxyMBean.class);
        long totalHints = storageProxy.getTotalHints();
        logger.info(String.format("Total hints found: %s", totalHints));
        return totalHints;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> ring(){
        return ring(null);
    }

    @SuppressWarnings ("unchecked")
    public List<Map<String, Object>> ring(String keyspace) {
        logger.info("JMX ring being called");
        List<Map<String, Object>> ring = Lists.newArrayList();
        Map<String, String> tokenToEndpoint = getTokenToEndpointMap();
        List<String> sortedTokens = new ArrayList<String>(tokenToEndpoint.keySet());

        Collection<String> liveNodes = getLiveNodes();
        Collection<String> deadNodes = getUnreachableNodes();
        Collection<String> joiningNodes = getJoiningNodes();
        Collection<String> leavingNodes = getLeavingNodes();
        Collection<String> movingNodes = getMovingNodes();
        Map<String, String> loadMap = getLoadMap();

        String format = "%-16s%-12s%-12s%-7s%-8s%-16s%-20s%-44s%n";

        // Calculate per-token ownership of the ring
        Map<String, Float> ownerships;
        try {
            if (Strings.isNullOrEmpty(keyspace)){
                ownerships = getOwnership();
            }
            else {
                ownerships = effectiveOwnership(keyspace);
            }
        } catch (ConfigurationException ex) {
            ownerships = getOwnership();
        }

        for (String token : sortedTokens) {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String dataCenter;
            try {
                dataCenter = getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
            } catch (UnknownHostException e) {
                dataCenter = "Unknown";
            }
            String rack;
            try {
                rack = getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
            } catch (UnknownHostException e) {
                rack = "Unknown";
            }
            String status = liveNodes.contains(primaryEndpoint)
                    ? "Up"
                    : deadNodes.contains(primaryEndpoint)
                    ? "Down"
                    : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint)) {
                state = "Joining";
            } else if (leavingNodes.contains(primaryEndpoint)) {
                state = "Leaving";
            } else if (movingNodes.contains(primaryEndpoint)) {
                state = "Moving";
            }

            String load = loadMap.containsKey(primaryEndpoint)
                    ? loadMap.get(primaryEndpoint)
                    : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token) == null ? 0.0F : ownerships.get(token));
            ring.add(createJson(primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
        logger.info(ring.toString());
        return ring;
    }

    private Map<String, Object> createJson(String primaryEndpoint, String dataCenter, String rack, String status, String state, String load, String owns, String token) {
        Map<String, Object> object = Maps.newHashMap();
        object.put("endpoint", primaryEndpoint);
        object.put("dc", dataCenter);
        object.put("rack", rack);
        object.put("status", status);
        object.put("state", state);
        object.put("load", load);
        object.put("owns", owns);
        object.put("token", token);
        return object;
    }

    public void compact() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) {
            forceTableCompaction(keyspace);
        }
    }

    public void repair(boolean isSequential) throws IOException {
        for (String keyspace : getKeyspaces()) {
            forceTableRepair(keyspace, isSequential);
        }
    }

    public void repairPrimaryRange(boolean isSequential) throws IOException {
        for (String keyspace : getKeyspaces()) {
            forceTableRepairPrimaryRange(keyspace, isSequential);
        }
    }

    public void repair(String keyspace, boolean isSequential) throws IOException, ExecutionException, InterruptedException {
        // Turns out that when a range is repaired, it is repaired on "all" replica. Therefore, it is redundant and in-efficient to do node repair on all ranges on all nodes.
        // Only repairing the primary range on each node in the cluster will repair the whole cluster
        forceTableRepairPrimaryRange(keyspace, isSequential, new String[0]);
    }

    public void cleanup() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) {
            if ("system".equals(keyspace)) {
                continue; // It is an error to attempt to cleanup the system column family.
            }
            forceTableCleanup(keyspace);
        }
    }

    public void flush() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) {
            forceTableFlush(keyspace);
        }
    }

    public void refresh(List<String> keyspaces) throws IOException {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = super.getColumnFamilyStoreMBeanProxies();
        while (it.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            if (keyspaces.contains(entry.getKey())) {
                logger.info("Refreshing " + entry.getKey() + " " + entry.getValue().getColumnFamilyName());
                loadNewSSTables(entry.getKey(), entry.getValue().getColumnFamilyName());
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (JMXNodeTool.class) {
            tool = null;
            super.close();
        }
    }
}
