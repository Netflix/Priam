package com.priam.conf;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

/**
 * This class provides a way to get the data out of the JMX.
 * 
 * @author "Vijay Parthasarathy"
 */
public class JMXNodeTool extends NodeProbe
{
    private static JMXNodeTool tool = null;
    /**
     * Hostname and Port to talk to will be same server for now optionally we
     * might want the ip to poll.
     * 
     * NOTE: This class shouldn't be a singleton and this shouldnt be cached.
     * 
     * This will work only if cassandra runs.
     */
    JMXNodeTool(String host, int port) throws IOException, InterruptedException
    {
        super(host, port);
    }

    public static JMXNodeTool instance(IConfiguration config)
    {
        // try to create if it is null.
        try
        {
            tool = new JMXNodeTool("localhost", config.getJmxPort());
        }
        catch (Exception ex)
        {
            // ignore this for now..
        }
        return tool;
    }

    public void retry() throws IOException
    {
        // super.connect();
    }

    /**
     * You must do the compaction before running this to remove the duplicate
     * tokens out of the server. TODO code it.
     */
    public String estimateKeys() throws JSONException
    {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = super.getColumnFamilyStoreMBeanProxies();
        JSONObject object = new JSONObject("ESTIMATED_KEYS");
        while (it.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            object.put("Keyspace", entry.getKey());
            object.put("Column Family", entry.getValue().getColumnFamilyName());
            object.put("Estimated Size", entry.getValue().estimateKeys());
        }
        return object.toString();
    }

    public String info() throws JSONException
    {
        JSONObject object = new JSONObject("INFO");
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
        return object.toString();
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

    // TODO move it test.
    public static void main(String[] args) throws Exception
    {
        System.err.println("WARNING: Do a manual compaction on the node before running this tool, to be more acurrate.... ");
        System.err.println("WARNING: If you have a lot of updates, this estimate will be way off... else it could be more accurate.... \n");
        System.err.println("Hint: Atleast run flush before running this tool to get data which is in memory... \n");
        if (args.length == 0)
        {
            System.out.println("USEAGE: command <hostname, port> \n");
        }

        String host;
        int port;
        if (args.length < 2)
        {
            System.out.println("Assuming the hostname to be: " + "127.0.0.1 and port 7199");
            System.out.println("================ \t================");
            host = "127.0.0.1";
            port = 7199;
        }
        else
        {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        System.out.println(new JMXNodeTool(host, port).estimateKeys());
    }

    public boolean isInitialized()
    {
        // TODO Auto-generated method stub
        return false;
    }
}
