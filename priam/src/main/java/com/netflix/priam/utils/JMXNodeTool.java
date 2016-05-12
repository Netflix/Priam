/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.utils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;

/**
 * Class to get data out of Cassandra JMX
 */
@Singleton
public class JMXNodeTool extends NodeProbe implements INodeToolObservable
{
    private static final Logger logger = LoggerFactory.getLogger(JMXNodeTool.class);
    private static volatile JMXNodeTool tool = null;
    private MBeanServerConnection mbeanServerConn = null;
    
    private static Set<INodeToolObserver> observers = new HashSet<INodeToolObserver>();

    /**
     * Hostname and Port to talk to will be same server for now optionally we
     * might want the ip to poll.
     * 
     * NOTE: This class shouldn't be a singleton and this shouldn't be cached.
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
     * @throws JMXConnectionException 
     * @throws IOException 
     */
    public static JMXNodeTool instance(IConfiguration config) throws JMXConnectionException
    {
        if (!testConnection())
            tool = connect(config);
        return tool;
    }

    public static <T> T getRemoteBean(Class<T> clazz, String mbeanName, IConfiguration config, boolean mxbean) throws JMXConnectionException, IOException, MalformedObjectNameException
    {
        if (mxbean)
            return ManagementFactory.newPlatformMXBeanProxy(JMXNodeTool.instance(config).mbeanServerConn, mbeanName, clazz);
        else
            return JMX.newMBeanProxy(JMXNodeTool.instance(config).mbeanServerConn, new ObjectName(mbeanName), clazz);

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

    /*
     * A means to clean up existing and recreate the JMX connection to the Cassandra process.
     * @return the new connection.
     */
    public static synchronized JMXNodeTool createNewConnection(final IConfiguration config) throws JMXConnectionException {
    	return createConnection(config);
    }    
    
    public static synchronized JMXNodeTool connect(final IConfiguration config) throws JMXConnectionException
    {
    	//lets make sure some other monitor didn't sneak in the recreated the connection already
		if (!testConnection()) {
			
        	if (tool != null) {
            	try {
    				tool.close(); //Ensure we properly close any existing (even if it's corrupted) connection to the remote jmx agent
    			} catch (IOException e) {
    				logger.warn("Exception performing house cleaning -- closing current connection to jmx remote agent.  Msg: " + e.getLocalizedMessage(), e);
    			}        		
        	}
        	
		} else {
			//Someone beat you and already created the connection, nothing you need to do..
			return tool;
		}
    	
		return createConnection(config);
    }
    
    private static JMXNodeTool createConnection(final IConfiguration config) throws JMXConnectionException {
    	JMXNodeTool jmxNodeTool = null;
		// If Cassandra is started then only start the monitoring
		if (!CassandraMonitor.isCassadraStarted()) {
			String exceptionMsg = "Cannot perform connection to remove jmx agent as Cassandra is not yet started, check back again later";
			logger.debug(exceptionMsg);
			throw new JMXConnectionException(exceptionMsg);
		}
		
    	if (tool != null) { //lets make sure we properly close any existing (even if it's corrupted) connection to the remote jmx agent 
        	try {
				tool.close();
			} catch (IOException e) {
				logger.warn("Exception performing house cleaning -- closing current connection to jmx remote agent.  Msg: " + e.getLocalizedMessage(), e);
			}        		
    	}
    		
		try {
			
			jmxNodeTool = new BoundedExponentialRetryCallable<JMXNodeTool>()
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
			}.call();
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new JMXConnectionException(e.getMessage());
		}
		
		logger.info("Connected to remote jmx agent, will notify interested parties!");
		Iterator<INodeToolObserver> it = observers.iterator();
		while (it.hasNext()) {
			INodeToolObserver observer = it.next();
			observer.nodeToolHasChanged(jmxNodeTool);
		}
		
		return jmxNodeTool;    	
    }

    /**
     * You must do the compaction before running this to remove the duplicate
     * tokens out of the server. TODO code it.
     */
    @SuppressWarnings("unchecked")
    public JSONObject estimateKeys() throws JSONException
    {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = super.getColumnFamilyStoreMBeanProxies();
        JSONObject object = new JSONObject();
        while (it.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            object.put("keyspace", entry.getKey());
            object.put("column_family", entry.getValue().getColumnFamilyName());
            object.put("estimated_size", entry.getValue().estimateKeys());
        }
        return object;
    }

    @SuppressWarnings("unchecked")
    public JSONObject info() throws JSONException
    {
        JSONObject object = new JSONObject();
        object.put("gossip_active", isInitialized());
        object.put("thrift_active", isThriftServerRunning());
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

    @SuppressWarnings("unchecked")
    public JSONArray ring(String keyspace) throws JSONException
    {
        JSONArray ring = new JSONArray();
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
        Map<InetAddress, Float> ownerships;
        try
        {
            ownerships = effectiveOwnership(keyspace);
        }
        catch (IllegalStateException ex)
        {
            ownerships = getOwnership();
        }

        for (String token : sortedTokens)
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
            String status = liveNodes.contains(primaryEndpoint)
                            ? "Up"
                            : deadNodes.contains(primaryEndpoint)
                              ? "Down"
                              : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint))
                state = "Joining";
            else if (leavingNodes.contains(primaryEndpoint))
                state = "Leaving";
            else if (movingNodes.contains(primaryEndpoint))
                state = "Moving";

            String load = loadMap.containsKey(primaryEndpoint)
                          ? loadMap.get(primaryEndpoint)
                          : "?";
            String owns = new DecimalFormat("##0.00%").format(ownerships.get(token) == null ? 0.0F : ownerships.get(token));
            ring.put(createJson(primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
        return ring;
    }

    private JSONObject createJson(String primaryEndpoint, String dataCenter, String rack, String status, String state, String load, String owns, String token) throws JSONException
    {
        JSONObject object = new JSONObject();
        object.put("endpoint", primaryEndpoint);
        object.put("dc", dataCenter);
        object.put("rack", rack);
        object.put("status", status);
        object.put("state", state);
        object.put("load", load);
        object.put("owns", owns);
        object.put("token", token.toString());
        return object;
    }

    public void compact() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces()) {
        	forceKeyspaceCompaction(keyspace, new String[0]);
        }
        	
    }

    public void repair(boolean isSequential, boolean localDataCenterOnly) throws IOException, ExecutionException, InterruptedException
    {
        repair(isSequential, localDataCenterOnly, false);
    }
    public void repair(boolean isSequential, boolean localDataCenterOnly, boolean primaryRange) throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
            if (primaryRange)
            	forceKeyspaceRepairPrimaryRange(keyspace, isSequential, localDataCenterOnly, new String[0]);
            else
            	forceKeyspaceRepair(keyspace, isSequential, localDataCenterOnly, new String[0]);
    }

    public void cleanup() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
        	forceKeyspaceCleanup(keyspace, new String[0]);
    }

    public void flush() throws IOException, ExecutionException, InterruptedException
    {
        for (String keyspace : getKeyspaces())
        	forceKeyspaceFlush(keyspace, new String[0]);
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
    
	/*
	 * @param observer to add to list of internal observers.  This behavior is thread-safe.
	 */
    @Override
	public void addObserver(INodeToolObserver observer) {
    	if (observer == null) 
    		throw new NullPointerException("Cannot not observer.");
    	synchronized(observers) {
    	   	observers.add(observer);  //if observer exist, it's a noop   		
    	}
    	
    }
    
    /*
     * @param observer to be removed; behavior is thread-safe.
     */
    @Override
    public void deleteObserver(INodeToolObserver observer) {
    	synchronized(observers) {
        	observers.remove(observer);    		
    	}
    }
}