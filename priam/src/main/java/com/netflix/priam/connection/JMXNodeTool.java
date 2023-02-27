/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.connection;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.CassandraMonitor;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to get data out of Cassandra JMX */
@Singleton
public class JMXNodeTool extends NodeProbe implements INodeToolObservable {
    private static final Logger logger = LoggerFactory.getLogger(JMXNodeTool.class);
    private static volatile JMXNodeTool tool = null;
    private MBeanServerConnection mbeanServerConn = null;

    private static final Set<INodeToolObserver> observers = new HashSet<>();

    /**
     * Hostname and Port to talk to will be same server for now optionally we might want the ip to
     * poll.
     *
     * <p>NOTE: This class shouldn't be a singleton and this shouldn't be cached.
     *
     * <p>This will work only if cassandra runs.
     */
    public JMXNodeTool(String host, int port) throws IOException, InterruptedException {
        super(host, port);
    }

    public JMXNodeTool(String host, int port, String username, String password)
            throws IOException, InterruptedException {
        super(host, port, username, password);
    }

    @Inject
    public JMXNodeTool(IConfiguration config) throws IOException, InterruptedException {
        super("localhost", config.getJmxPort());
    }

    /**
     * try to create if it is null.
     *
     * @throws JMXConnectionException
     */
    public static JMXNodeTool instance(IConfiguration config) throws JMXConnectionException {
        if (!testConnection()) tool = connect(config);
        return tool;
    }

    public static <T> T getRemoteBean(
            Class<T> clazz, String mbeanName, IConfiguration config, boolean mxbean)
            throws IOException, MalformedObjectNameException {
        if (mxbean)
            return ManagementFactory.newPlatformMXBeanProxy(
                    JMXNodeTool.instance(config).mbeanServerConn, mbeanName, clazz);
        else
            return JMX.newMBeanProxy(
                    JMXNodeTool.instance(config).mbeanServerConn, new ObjectName(mbeanName), clazz);
    }

    /**
     * Returns plain MBeanServer Connection
     *
     * @param config Configuration to initialize JMX Connection
     * @return MBeanServerConnection
     * @throws JMXConnectionException
     */
    public static MBeanServerConnection getMbeanServerConn(IConfiguration config)
            throws JMXConnectionException {
        return JMXNodeTool.instance(config).mbeanServerConn;
    }

    /**
     * This method will test if you can connect and query something before handing over the
     * connection, This is required for our retry logic.
     *
     * @return
     */
    private static boolean testConnection() {
        // connecting first time hence return false.
        if (tool == null) return false;

        try {
            MBeanServerConnection serverConn = tool.mbeanServerConn;
            if (serverConn == null) {
                logger.info(
                        "Test connection to remove MBean server failed as there is no connection.");
                return false;
            }

            if (serverConn.getMBeanCount()
                    < 1) { // If C* is up, it should have at multiple MBeans registered.
                logger.info(
                        "Test connection to remove MBean server failed as there is no registered MBeans.");
                return false;
            }
        } catch (Throwable ex) {
            closeQuietly(tool);
            logger.error(
                    "Exception while checking JMX connection to C*, msg: {}",
                    ex.getLocalizedMessage());
            return false;
        }
        return true;
    }

    private static void closeQuietly(JMXNodeTool tool) {
        try {
            tool.close();
        } catch (Exception e) {
            logger.warn("failed to close jmx node tool", e);
        }
    }

    /**
     * A means to clean up existing and recreate the JMX connection to the Cassandra process.
     *
     * @return the new connection.
     */
    public static synchronized JMXNodeTool createNewConnection(final IConfiguration config)
            throws JMXConnectionException {
        return createConnection(config);
    }

    public static synchronized JMXNodeTool connect(final IConfiguration config)
            throws JMXConnectionException {
        // lets make sure some other monitor didn't sneak in the recreated the connection already
        if (!testConnection()) {

            if (tool != null) {
                try {
                    tool.close(); // Ensure we properly close any existing (even if it's
                    // corrupted) connection to the remote jmx agent
                } catch (IOException e) {
                    logger.warn(
                            "Exception performing house cleaning -- closing current connection to jmx remote agent.  Msg: {}",
                            e.getLocalizedMessage(),
                            e);
                }
            }

        } else {
            // Someone beat you and already created the connection, nothing you need to do..
            return tool;
        }

        return createConnection(config);
    }

    private static JMXNodeTool createConnection(final IConfiguration config)
            throws JMXConnectionException {
        // If Cassandra is started then only start the monitoring
        if (!CassandraMonitor.hasCassadraStarted()) {
            String exceptionMsg =
                    "Cannot perform connection to remove jmx agent as Cassandra has not yet started, check back again later";
            logger.debug(exceptionMsg);
            throw new JMXConnectionException(exceptionMsg);
        }

        if (tool
                != null) { // lets make sure we properly close any existing (even if it's corrupted)
            // connection to the remote jmx agent
            try {
                tool.close();
            } catch (IOException e) {
                logger.warn(
                        "Exception performing house cleaning -- closing current connection to jmx remote agent.  Msg: {}",
                        e.getLocalizedMessage(),
                        e);
            }
        }

        try {

            tool =
                    new BoundedExponentialRetryCallable<JMXNodeTool>() {
                        @Override
                        public JMXNodeTool retriableCall() throws Exception {
                            JMXNodeTool nodetool;
                            if ((config.getJmxUsername() == null
                                            || config.getJmxUsername().isEmpty())
                                    && (config.getJmxPassword() == null
                                            || config.getJmxPassword().isEmpty())) {
                                nodetool = new JMXNodeTool("localhost", config.getJmxPort());
                            } else {
                                nodetool =
                                        new JMXNodeTool(
                                                "localhost",
                                                config.getJmxPort(),
                                                config.getJmxUsername(),
                                                config.getJmxPassword());
                            }

                            Field fields[] = NodeProbe.class.getDeclaredFields();
                            for (Field field : fields) {
                                if (!field.getName().equals("mbeanServerConn")) continue;
                                field.setAccessible(true);
                                nodetool.mbeanServerConn =
                                        (MBeanServerConnection) field.get(nodetool);
                            }

                            return nodetool;
                        }
                    }.call();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new JMXConnectionException(e.getMessage());
        }

        logger.info("Connected to remote jmx agent, will notify interested parties!");
        for (INodeToolObserver observer : observers) {
            observer.nodeToolHasChanged(tool);
        }

        return tool;
    }

    /**
     * You must do the compaction before running this to remove the duplicate tokens out of the
     * server. TODO code it.
     */
    public JSONObject estimateKeys() throws JSONException {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it =
                super.getColumnFamilyStoreMBeanProxies();
        JSONObject object = new JSONObject();
        while (it.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            object.put("keyspace", entry.getKey());
            object.put("column_family", entry.getValue().getColumnFamilyName());
            object.put("estimated_size", entry.getValue().estimateKeys());
        }
        return object;
    }

    public JSONObject info() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("gossip_active", isInitialized());
        object.put("thrift_active", isThriftServerRunning());
        object.put("native_active", isNativeTransportRunning());
        object.put("token", getTokens().toString());
        object.put("load", getLoadString());
        object.put("generation_no", getCurrentGenerationNumber());
        object.put("uptime", getUptime() / 1000);
        MemoryUsage heapUsage = getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        object.put("heap_memory_mb", memUsed + "/" + memMax);
        object.put("data_center", getDataCenter());
        object.put("rack", getRack());
        return object;
    }

    public JSONObject statusInformation() throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("live", getLiveNodes());
        jsonObject.put("unreachable", getUnreachableNodes());
        jsonObject.put("joining", getJoiningNodes());
        jsonObject.put("leaving", getLeavingNodes());
        jsonObject.put("moving", getMovingNodes());
        jsonObject.put("tokenToEndpointMap", getTokenToEndpointMap());
        return jsonObject;
    }

    public JSONArray ring(String keyspace) throws JSONException {
        JSONArray ring = new JSONArray();
        Map<String, String> tokenToEndpoint = getTokenToEndpointMap();
        List<String> sortedTokens = new ArrayList<>(tokenToEndpoint.keySet());

        Collection<String> liveNodes = getLiveNodes();
        Collection<String> deadNodes = getUnreachableNodes();
        Collection<String> joiningNodes = getJoiningNodes();
        Collection<String> leavingNodes = getLeavingNodes();
        Collection<String> movingNodes = getMovingNodes();
        Map<String, String> loadMap = getLoadMap();

        String format = "%-16s%-12s%-12s%-7s%-8s%-16s%-20s%-44s%n";

        // Calculate per-token ownership of the ring
        Map<InetAddress, Float> ownerships;
        try {
            ownerships = effectiveOwnership(keyspace);
        } catch (IllegalStateException ex) {
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
            String status =
                    liveNodes.contains(primaryEndpoint)
                            ? "Up"
                            : deadNodes.contains(primaryEndpoint) ? "Down" : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint)) state = "Joining";
            else if (leavingNodes.contains(primaryEndpoint)) state = "Leaving";
            else if (movingNodes.contains(primaryEndpoint)) state = "Moving";

            String load = loadMap.getOrDefault(primaryEndpoint, "?");
            String owns =
                    new DecimalFormat("##0.00%")
                            .format(ownerships.get(token) == null ? 0.0F : ownerships.get(token));
            ring.put(
                    createJson(
                            primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
        return ring;
    }

    private JSONObject createJson(
            String primaryEndpoint,
            String dataCenter,
            String rack,
            String status,
            String state,
            String load,
            String owns,
            String token)
            throws JSONException {
        JSONObject object = new JSONObject();
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

    public void repair(boolean isSequential, boolean localDataCenterOnly)
            throws IOException, ExecutionException, InterruptedException {
        repair(isSequential, localDataCenterOnly, false);
    }

    public void repair(boolean isSequential, boolean localDataCenterOnly, boolean primaryRange)
            throws IOException, ExecutionException, InterruptedException {
        /**
         * ** Replace with this in 3.10 cassandra-all. Map<String, String> repairOptions = new
         * HashMap<>(); String isParallel = !isSequential?"true":"false";
         * repairOptions.put(RepairOption.PARALLELISM_KEY, isParallel);
         * repairOptions.put(RepairOption.PRIMARY_RANGE_KEY, primaryRange+""); if
         * (localDataCenterOnly) repairOptions.put(RepairOption.DATACENTERS_KEY, getDataCenter());
         */
        PrintStream printStream = new PrintStream("repair.log");
        Set<String> datacenters = null;
        if (localDataCenterOnly) {
            datacenters = new HashSet<>();
            datacenters.add(getDataCenter());
        }

        for (String keyspace : getKeyspaces())
            forceRepairAsync(
                    printStream, keyspace, isSequential, datacenters, null, primaryRange, true);
        /*if (primaryRange)
            forceKeyspaceRepairPrimaryRange(keyspace, isSequential, localDataCenterOnly, new String[0]);
        else
        	forceKeyspaceRepair(keyspace, isSequential, localDataCenterOnly, new String[0]);*/

    }

    public void cleanup() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) forceKeyspaceCleanup(0, keyspace);
        // forceKeyspaceCleanup(keyspace, new String[0]);
    }

    public void setIncrementalBackupsEnabled(boolean enabled) {
        super.setIncrementalBackupsEnabled(enabled);
    }

    public boolean isIncrementalBackupsEnabled() {
        return super.isIncrementalBackupsEnabled();
    }

    public void refresh(List<String> keyspaces)
            throws IOException, ExecutionException, InterruptedException {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it =
                super.getColumnFamilyStoreMBeanProxies();
        while (it.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            if (keyspaces.contains(entry.getKey())) {
                logger.info(
                        "Refreshing {} {}", entry.getKey(), entry.getValue().getColumnFamilyName());
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

    /** @param observer to add to list of internal observers. This behavior is thread-safe. */
    @Override
    public void addObserver(INodeToolObserver observer) {
        if (observer == null) throw new NullPointerException("Cannot not observer.");
        synchronized (observers) {
            observers.add(observer); // if observer exist, it's a noop
        }
    }

    /** @param observer to be removed; behavior is thread-safe. */
    @Override
    public void deleteObserver(INodeToolObserver observer) {
        synchronized (observers) {
            observers.remove(observer);
        }
    }
}
