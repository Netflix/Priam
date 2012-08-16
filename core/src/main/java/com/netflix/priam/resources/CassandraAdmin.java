package com.netflix.priam.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.SystemUtils;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Do general operations. Start/Stop and some JMX node tool commands
 */
@Path ("/v1/cassadmin")
@Produces (MediaType.APPLICATION_JSON)
public class CassandraAdmin {
    private static final String REST_HEADER_KEYSPACES = "keyspaces";
    private static final String REST_HEADER_CFS = "cfnames";
    private static final String REST_HEADER_TOKEN = "token";
    private static final Map<String, String> REST_SUCCESS = ImmutableMap.of("result", "ok");

    private static final Logger logger = LoggerFactory.getLogger(CassandraAdmin.class);

    private CassandraConfiguration cassandraConfiguration;
    private AmazonConfiguration amazonConfiguration;
    private BackupConfiguration backupConfiguration;

    @Inject
    public CassandraAdmin(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, BackupConfiguration backupConfiguration) {
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.backupConfiguration = backupConfiguration;
    }

    @GET
    @Path ("/start")
    public Response cassStart() throws IOException, InterruptedException {
        SystemUtils.startCassandra(true, cassandraConfiguration, backupConfiguration, amazonConfiguration.getInstanceType());
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/stop")
    public Response cassStop() throws IOException, InterruptedException {
        SystemUtils.stopCassandra(cassandraConfiguration);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/refresh")
    public Response cassRefresh(@QueryParam (REST_HEADER_KEYSPACES) String keyspaces) throws IOException, ExecutionException, InterruptedException {
        logger.info("node tool refresh is being called");
        if (StringUtils.isBlank(keyspaces)) {
            return Response.status(400).entity("Missing keyspace in request").build();
        }

        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        nodetool.refresh(Lists.newArrayList(keyspaces.split(",")));
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/info")
    public Response cassInfo() throws IOException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        logger.info("node tool info being called");
        return Response.ok(nodetool.info(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/ring/{id}")
    public Response cassRing(@PathParam ("id") String keyspace) throws IOException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        logger.info("node tool ring being called");
        return Response.ok(nodetool.ring(keyspace), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/flush")
    public Response cassFlush() throws IOException, InterruptedException, ExecutionException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        logger.info("node tool flush being called");
        nodetool.flush();
        return Response.ok().build();
    }

    @GET
    @Path ("/compact")
    public Response cassCompact() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        logger.info("node tool compact being called");
        nodetool.compact();
        return Response.ok().build();
    }

    @GET
    @Path ("/cleanup")
    public Response cassCleanup() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        logger.info("node tool cleanup being called");
        nodetool.cleanup();
        return Response.ok().build();
    }

    @GET
    @Path ("/repair")
    public Response cassRepair(@QueryParam ("sequential") boolean isSequential) throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        logger.info("node tool repair being called");
        nodetool.repair(isSequential);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/version")
    public Response version() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        return Response.ok(ImmutableMap.of("version", nodetool.getReleaseVersion()), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/tpstats")
    public Response tpstats() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = nodetool.getThreadPoolMBeanProxies();
        List<Map<String, Object>> threadPoolArray = Lists.newArrayList();
        while (threads.hasNext()) {
            Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
            Map<String, Object> tpObj = Maps.newHashMap();  // "Pool Name", "Active",
            // "Pending", "Completed",
            // "Blocked", "All time blocked"
            tpObj.put("pool name", thread.getKey());
            tpObj.put("active", threadPoolProxy.getActiveCount());
            tpObj.put("pending", threadPoolProxy.getPendingTasks());
            tpObj.put("completed", threadPoolProxy.getCompletedTasks());
            tpObj.put("blocked", threadPoolProxy.getCurrentlyBlockedTasks());
            tpObj.put("total blocked", threadPoolProxy.getTotalBlockedTasks());
            threadPoolArray.add(tpObj);
        }
        Map<String, Object> droppedMsgs = Maps.newHashMap();
        for (Entry<String, Integer> entry : nodetool.getDroppedMessages().entrySet()) {
            droppedMsgs.put(entry.getKey(), entry.getValue());
        }

        Map<String, Object> rootObj = Maps.newHashMap();
        rootObj.put("thread pool", threadPoolArray);
        rootObj.put("dropped messages", droppedMsgs);

        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/compactionstats")
    public Response compactionStats()  throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        Map<String, Object> rootObj = Maps.newHashMap();
        CompactionManagerMBean cm = nodetool.getCompactionManagerProxy();
        rootObj.put("pending tasks", cm.getPendingTasks());
        List<Map<String, Object>> compStats = Lists.newArrayList();
        for (Map<String, String> c : cm.getCompactions()) {
            Map<String, Object> cObj = Maps.newHashMap();
            cObj.put("id", c.get("id"));
            cObj.put("keyspace", c.get("keyspace"));
            cObj.put("columnfamily", c.get("columnfamily"));
            cObj.put("bytesComplete", c.get("bytesComplete"));
            cObj.put("totalBytes", c.get("totalBytes"));
            cObj.put("taskType", c.get("taskType"));
            String percentComplete = new Long(c.get("totalBytes")) == 0 ? "n/a" : new DecimalFormat("0.00").format((double) new Long(c.get("bytesComplete")) / new Long(c.get("totalBytes")) * 100) + "%";
            cObj.put("progress", percentComplete);
            compStats.add(cObj);
        }
        rootObj.put("compaction stats", compStats);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/disablegossip")
    public Response disablegossip() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        nodetool.stopGossiping();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/enablegossip")
    public Response enablegossip() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        nodetool.startGossiping();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/disablethrift")
    public Response disablethrift() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        nodetool.stopThriftServer();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/enablethrift")
    public Response enablethrift() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        nodetool.startThriftServer();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/statusthrift")
    public Response statusthrift() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        return Response.ok(ImmutableMap.of("status", (nodetool.isThriftServerRunning() ? "running" : "not running")), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/gossipinfo")
    public Response gossipinfo() throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        Map<String, Object> rootObj = Maps.newHashMap();
        String[] ginfo = nodetool.getGossipInfo().split("/");
        for (String info : ginfo) {
            String[] data = info.split("\n");
            String key = "";
            Map<String, Object> obj = Maps.newHashMap();
            for (String element : data) {
                String[] kv = element.split(":");
                if (kv.length == 1) {
                    key = kv[0];
                } else {
                    obj.put(kv[0], kv[1]);
                }
            }
            if (StringUtils.isNotBlank(key)) {
                rootObj.put(key, obj);
            }
        }
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/netstats")
    public Response netstats(@QueryParam ("host") String hostname) throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        Map<String, Object> rootObj = Maps.newHashMap();
        rootObj.put("mode", nodetool.getOperationMode());
        final InetAddress addr = (hostname == null) ? null : InetAddress.getByName(hostname);
        Set<InetAddress> hosts = addr == null ? nodetool.getStreamDestinations() : new HashSet<InetAddress>() {
            {
                add(addr);
            }
        };
        if (hosts.size() == 0) {
            rootObj.put("sending", "Not sending any streams.");
        }

        Map<String, Object> hostSendStats = Maps.newHashMap();
        for (InetAddress host : hosts) {
            try {
                List<String> files = nodetool.getFilesDestinedFor(host);
                if (files.size() > 0) {
                    List<String> fObj = Lists.newArrayList();
                    for (String file : files) {
                        fObj.add(file);
                    }
                    hostSendStats.put(host.getHostAddress(), fObj);
                }
            } catch (IOException ex) {
                hostSendStats.put(host.getHostAddress(), "Error retrieving file data");
            }
        }

        rootObj.put("hosts sending", hostSendStats);
        hosts = addr == null ? nodetool.getStreamSources() : new HashSet<InetAddress>() {
            {
                add(addr);
            }
        };
        if (hosts.size() == 0) {
            rootObj.put("receiving", "Not receiving any streams.");
        }

        Map<String, Object> hostRecvStats = Maps.newHashMap();
        for (InetAddress host : hosts) {
            try {
                List<String> files = nodetool.getIncomingFiles(host);
                if (files.size() > 0) {
                    List<String> fObj = Lists.newArrayList();
                    for (String file : files) {
                        fObj.add(file);
                    }
                    hostRecvStats.put(host.getHostAddress(), fObj);
                }
            } catch (IOException ex) {
                hostRecvStats.put(host.getHostAddress(), "Error retrieving file data");
            }
        }
        rootObj.put("hosts receiving", hostRecvStats);

        MessagingServiceMBean ms = nodetool.msProxy;
        int pending;
        long completed;
        pending = 0;
        for (int n : ms.getCommandPendingTasks().values()) {
            pending += n;
        }
        completed = 0;
        for (long n : ms.getCommandCompletedTasks().values()) {
            completed += n;
        }
        Map<String, Object> cObj = Maps.newHashMap();
        cObj.put("active", "n/a");
        cObj.put("pending", pending);
        cObj.put("completed", completed);
        rootObj.put("commands", cObj);

        pending = 0;
        for (int n : ms.getResponsePendingTasks().values()) {
            pending += n;
        }
        completed = 0;
        for (long n : ms.getResponseCompletedTasks().values()) {
            completed += n;
        }
        Map<String, Object> rObj = Maps.newHashMap();
        rObj.put("active", "n/a");
        rObj.put("pending", pending);
        rObj.put("completed", completed);
        rootObj.put("responses", rObj);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/move")
    public Response moveToken(@QueryParam (REST_HEADER_TOKEN) String newToken) throws IOException, ExecutionException, InterruptedException, ConfigurationException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        nodetool.move(newToken);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/scrub")
    public Response scrub(@QueryParam (REST_HEADER_KEYSPACES) String keyspaces, @QueryParam (REST_HEADER_CFS) String cfnames) throws IOException, ExecutionException, InterruptedException, ConfigurationException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        String[] cfs = null;
        if (StringUtils.isNotBlank(cfnames)) {
            cfs = cfnames.split(",");
        }
        if (cfs == null) {
            nodetool.scrub(keyspaces);
        } else {
            nodetool.scrub(keyspaces, cfs);
        }
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path ("/cfhistograms")
    public Response cfhistograms(@QueryParam (REST_HEADER_KEYSPACES) String keyspace, @QueryParam (REST_HEADER_CFS) String cfname) throws IOException, ExecutionException, InterruptedException {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        if (StringUtils.isBlank(keyspace) || StringUtils.isBlank(cfname)) {
            return Response.status(400).entity("Missing keyspace/cfname in request").build();
        }

        ColumnFamilyStoreMBean store = nodetool.getCfsProxy(keyspace, cfname);

        // default is 90 offsets
        long[] offsets = new EstimatedHistogram().getBucketOffsets();

        long[] recentReadLatencyHistMicros = store.getRecentReadLatencyHistogramMicros();
        long[] recentWriteLatencyHistMicros = store.getRecentWriteLatencyHistogramMicros();
        long[] recentSSTablesPerReadHist = store.getRecentSSTablesPerReadHistogram();
        long[] estimatedRowSizeHist = store.getEstimatedRowSizeHistogram();
        long[] estimatedColumnCountHist = store.getEstimatedColumnCountHistogram();

        Map<String, Object> rootObj = Maps.newHashMap();
        List<String> columns = ImmutableList.of("offset", "sstables", "write latency", "read latency", "row size", "column count");
        rootObj.put("columns", columns);
        List<Object> values = Lists.newArrayList();
        for (int i = 0; i < offsets.length; i++) {
            List<Object> row = Lists.newArrayList();
            row.add(offsets[i]);
            row.add(i < recentSSTablesPerReadHist.length ? recentSSTablesPerReadHist[i] : "");
            row.add(i < recentWriteLatencyHistMicros.length ? recentWriteLatencyHistMicros[i] : "");
            row.add(i < recentReadLatencyHistMicros.length ? recentReadLatencyHistMicros[i] : "");
            row.add(i < estimatedRowSizeHist.length ? estimatedRowSizeHist[i] : "");
            row.add(i < estimatedColumnCountHist.length ? estimatedColumnCountHist[i] : "");
            values.add(row);
        }
        rootObj.put("values", values);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

}
