package com.netflix.priam.utils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.cache.InstrumentingCacheMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tools.NodeProbe;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;

/**
 * Class to get data out of Cassandra JMX
 */
@Singleton
public class JMXNodeTool extends NodeProbe
{
    private static final Logger logger = LoggerFactory.getLogger(JMXNodeTool.class);
    private static String keyCacheObjFmt = "org.apache.cassandra.db:type=Caches,keyspace=%s,cache=%sKeyCache";
    private static String rowCacheObjFmt = "org.apache.cassandra.db:type=Caches,keyspace=%s,cache=%sRowCache";

    private static volatile JMXNodeTool tool = null;
    private MBeanServerConnection mbeanServerConn = null;

    /**
     * Hostname and Port to talk to will be same server for now optionally we
     * might want the ip to poll.
     * 
     * NOTE: This class shouldn't be a singleton and this shouldnt be cached.
     * 
     * This will work only if cassandra runs.
     */
    public JMXNodeTool(String host, int port) throws IOException, InterruptedException
    {
        super(host, port);
    }

    @Inject
    public JMXNodeTool(IConfiguration config) throws IOException, InterruptedException
    {
        super("localhost", config.getJmxPort());
    }

    /**
     * try to create if it is null.
     */
    public static JMXNodeTool instance(IConfiguration config)
    {
        if (!testConnection())
            tool = connect(config);
        return tool;
    }

    public static <T> T getRemoteBean(Class<T> clazz, String mbeanName, IConfiguration config, boolean mxbean)
    {
        try
        {
            if (mxbean)
                return ManagementFactory.newPlatformMXBeanProxy(JMXNodeTool.instance(config).mbeanServerConn, mbeanName, clazz);
            else
                return JMX.newMBeanProxy(JMXNodeTool.instance(config).mbeanServerConn, new ObjectName(mbeanName), clazz);
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * This method will test if you can connect and query something before handing over the connection,
     * This is required for our retry logic.
     * @return
     */
    private static boolean testConnection()
    {
        // connecting first time hence return false.
        if (tool == null)
            return false;
        
        try
        {
            tool.isInitialized();
        }
        catch (Throwable ex)
        {
            SystemUtils.closeQuietly(tool);
            return false;
        }
        return true;
    }

    public static synchronized JMXNodeTool connect(final IConfiguration config)
    {
        return SystemUtils.retryForEver(new RetryableCallable<JMXNodeTool>()
        {
            @Override
            public JMXNodeTool retriableCall() throws Exception
            {
                JMXNodeTool nodetool = new JMXNodeTool("localhost", config.getJmxPort());
                Field fields[] = NodeProbe.class.getDeclaredFields();
                for (int i = 0; i < fields.length; i++)
                {
                    if (!fields[i].getName().equals("mbeanServerConn"))
                        continue;
                    fields[i].setAccessible(true);
                    nodetool.mbeanServerConn = (MBeanServerConnection) fields[i].get(nodetool);
                }
                return nodetool;
            }
        });
    }

    /**
     * You must do the compaction before running this to remove the duplicate
     * tokens out of the server. TODO code it.
     */
    @SuppressWarnings("unchecked")
    public JSONObject estimateKeys()
    {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = super.getColumnFamilyStoreMBeanProxies();
        JSONObject object = new JSONObject();
        while (it.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            object.put("Keyspace", entry.getKey());
            object.put("Column Family", entry.getValue().getColumnFamilyName());
            object.put("Estimated Size", entry.getValue().estimateKeys());
        }
        return object;
    }

    @SuppressWarnings("unchecked")
    public JSONObject info()
    {
        logger.info("JMX info being called");
        JSONObject object = new JSONObject();
        object.put("INTIALIZED", isInitialized());
        object.put("Token", getToken());
        object.put("Load", getLoadString());
        object.put("Generation No", getCurrentGenerationNumber());
        object.put("Uptime (seconds)", getUptime() / 1000);
        MemoryUsage heapUsage = getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        object.put("Heap Memory (MB)", memUsed + "/" + memMax);
        object.put("Data Center", getDataCenter());
        object.put("Rack", getRack());
        // object.put("Exceptions", getExceptionCount());
        // return object.toString();
        logger.info(object.toString());
        return object;
    }

    @SuppressWarnings("unchecked")
    public JSONArray ring()
    {
        logger.info("JMX ring being called");
        JSONArray ring = new JSONArray();
        Map<Token, String> tokenToEndpoint = getTokenToEndpointMap();
        List<Token> sortedTokens = new ArrayList<Token>(tokenToEndpoint.keySet());
        Collections.sort(sortedTokens);

        Collection<String> liveNodes = getLiveNodes();
        Collection<String> deadNodes = getUnreachableNodes();
        Collection<String> joiningNodes = getJoiningNodes();
        Collection<String> leavingNodes = getLeavingNodes();
        Collection<String> movingNodes = getMovingNodes();
        Map<String, String> loadMap = getLoadMap();
        // Calculate per-token ownership of the ring
        Map<Token, Float> ownerships = getOwnership();

        for (Token token : sortedTokens)
        {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String dataCenter;
            try
            {
                dataCenter = getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
            }
            catch (UnknownHostException e)
            {
                dataCenter = "Unknown";
            }
            String rack;
            try
            {
                rack = getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
            }
            catch (UnknownHostException e)
            {
                rack = "Unknown";
            }
            String status = liveNodes.contains(primaryEndpoint) ? "Up" : deadNodes.contains(primaryEndpoint) ? "Down" : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint))
                state = "Joining";
            else if (leavingNodes.contains(primaryEndpoint))
                state = "Leaving";
            else if (movingNodes.contains(primaryEndpoint))
                state = "Moving";

            String load = loadMap.containsKey(primaryEndpoint) ? loadMap.get(primaryEndpoint) : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token));
            ring.add(createJson(primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
        // return ring.toString();
        logger.info(ring.toString());
        return ring;
    }

    @SuppressWarnings("unchecked")
    private JSONObject createJson(String primaryEndpoint, String dataCenter, String rack, String status, String state, String load, String owns, Token token)
    {
        JSONObject object = new JSONObject();
        object.put("ENDPOINT", primaryEndpoint);
        object.put("DC", dataCenter);
        object.put("RACK", rack);
        object.put("STATUS", status);
        object.put("STATE", state);
        object.put("LOAD", load);
        object.put("OWNS", owns);
        object.put("TOKEN", token.toString());
        return object;
    }

    public void compact() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
            forceTableCompaction(keyspace, new String[0]);
    }

    public void repair() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
            forceTableRepair(keyspace, new String[0]);
    }

    public void cleanup() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
            forceTableCleanup(keyspace, new String[0]);
    }

    public void flush() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
            forceTableFlush(keyspace, new String[0]);
    }

    public void refresh(List<String> keyspaces) throws IOException, ExecutionException, InterruptedException
    {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = super.getColumnFamilyStoreMBeanProxies();
        while (it.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            if (keyspaces.contains(entry.getKey()))
            {
                logger.info("Refreshing " + entry.getKey() + " " + entry.getValue().getColumnFamilyName());
                loadNewSSTables(entry.getKey(), entry.getValue().getColumnFamilyName());
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        synchronized (JMXNodeTool.class)
        {
            tool = null;
            super.close();
        }
    }

    public Iterator<Map.Entry<String, InstrumentingCacheMBean>> getKeyCacheMBeanProxies(IConfiguration config)
    {
        try
        {
            return new CacheMBeanIterator(mbeanServerConn, keyCacheObjFmt);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Invalid ObjectName", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not retrieve list of stat mbeans.", e);
        }
    }

    public Iterator<Map.Entry<String, InstrumentingCacheMBean>> getRowCacheMBeanProxies(IConfiguration config)
    {
        try
        {
            return new CacheMBeanIterator(mbeanServerConn, rowCacheObjFmt);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Invalid ObjectName.", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not retrieve list of stat mbeans.", e);
        }
    }

    class CacheMBeanIterator implements Iterator<Map.Entry<String, InstrumentingCacheMBean>>
    {
        private Iterator<ObjectName> resIter;
        private MBeanServerConnection mbeanServerConn;
        private String cachePath;

        public CacheMBeanIterator(MBeanServerConnection mbeanServerConn, String cachePath) throws MalformedObjectNameException, NullPointerException, IOException
        {
            ObjectName query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,*");
            resIter = mbeanServerConn.queryNames(query, null).iterator();
            this.mbeanServerConn = mbeanServerConn;
            this.cachePath = cachePath;
        }

        public boolean hasNext()
        {
            return resIter.hasNext();
        }

        public Entry<String, InstrumentingCacheMBean> next()
        {
            ObjectName objectName = resIter.next();
            String tableName = objectName.getKeyProperty("keyspace");
            String cfName = objectName.getKeyProperty("columnfamily");
            String keyCachePath = String.format(cachePath, tableName, cfName);
            InstrumentingCacheMBean cacheProxy = null;
            try
            {
                cacheProxy = JMX.newMBeanProxy(mbeanServerConn, new ObjectName(keyCachePath), InstrumentingCacheMBean.class);
            }
            catch (Exception e)
            {
                logger.error("Cannot get cache MBean", e);
            }
            return new AbstractMap.SimpleImmutableEntry<String, InstrumentingCacheMBean>(tableName + "_" + cfName, cacheProxy);
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
