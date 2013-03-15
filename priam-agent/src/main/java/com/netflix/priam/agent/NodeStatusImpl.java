package com.netflix.priam.agent;

import com.netflix.priam.utils.JMXNodeTool;
import org.apache.cassandra.config.ConfigurationException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class NodeStatusImpl implements NodeStatus
{
    private final JMXNodeTool nodeTool;

    public NodeStatusImpl(JMXNodeTool nodeTool)
    {
        this.nodeTool = nodeTool;
    }

    @Override
    public JSONObject info() throws JSONException
    {
        return nodeTool.info();
    }

    @Override
    public boolean isJoined()
    {
        return nodeTool.isJoined();
    }

    @Override
    public String getEndpoint()
    {
        return nodeTool.getEndpoint();
    }

    @Override
    public int getExceptionCount()
    {
        return nodeTool.getExceptionCount();
    }

    @Override
    public List<String> getLiveNodes()
    {
        return nodeTool.getLiveNodes();
    }

    @Override
    public List<String> getMovingNodes()
    {
        return nodeTool.getMovingNodes();
    }

    @Override
    public List<String> getJoiningNodes()
    {
        return nodeTool.getJoiningNodes();
    }

    @Override
    public List<String> getUnreachableNodes()
    {
        return nodeTool.getUnreachableNodes();
    }

    @Override
    public String getOperationMode()
    {
        return nodeTool.getOperationMode();
    }

    @Override
    public String getGossipInfo()
    {
        return nodeTool.getGossipInfo();
    }

    @Override
    public int getCompactionThroughput()
    {
        return nodeTool.getCompactionThroughput();
    }

    @Override
    public void invalidateKeyCache() throws IOException
    {
        nodeTool.invalidateKeyCache();
    }

    @Override
    public void compact() throws InterruptedException, ExecutionException, IOException
    {
        nodeTool.compact();
    }

    @Override
    public void decommission() throws InterruptedException
    {
        nodeTool.decommission();
    }

    @Override
    public void cleanup() throws InterruptedException, ExecutionException, IOException
    {
        nodeTool.cleanup();
    }

    @Override
    public void joinRing() throws IOException
    {
        try
        {
            nodeTool.joinRing();
        } catch (ConfigurationException e)
        {
            throw new IllegalArgumentException("Bad cassandra config", e);
        }
    }

    @Override
    public void refresh(List<String> keyspaces) throws InterruptedException, ExecutionException, IOException
    {
        nodeTool.refresh(keyspaces);
    }

    @Override
    public void removeNode(String token) throws Exception
    {
        nodeTool.removeToken(token);
    }

    @Override
    public void startThriftServer() throws Exception
    {
        nodeTool.startThriftServer();
    }

    @Override
    public void move(String token) throws Exception
    {
        nodeTool.move(token);
    }

    @Override
    public void flush() throws Exception
    {
        nodeTool.flush();
    }

    @Override
    public void repair(boolean sequential) throws Exception
    {
        nodeTool.repair(sequential);
    }

    @Override
    public void invalidateRowCache() throws Exception
    {
        nodeTool.invalidateRowCache();
    }

    @Override
    public void stopGossiping() throws Exception
    {
        nodeTool.stopGossiping();
    }

    @Override
    public void startGossiping() throws Exception
    {
        nodeTool.startGossiping();
    }

    @Override
    public void stopThriftServer() throws Exception
    {
        nodeTool.stopThriftServer();
    }

    @Override
    public void drain() throws Exception
    {
        nodeTool.drain();
    }
}
