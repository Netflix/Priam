package com.netflix.priam.agent;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.codehaus.jettison.json.JSONObject;
import javax.inject.Provider;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MockNodeStatus implements NodeStatus, Provider<NodeStatus>
{
    private JSONObject info = new JSONObject();
    private List<String> liveNodes = Lists.newArrayList();
    private List<String> movingNodes = Lists.newArrayList();
    private List<String> joiningNodes = Lists.newArrayList();
    private List<String> unreachableNodes = Lists.newArrayList();
    private String operationMode = "";
    private String gossipInfo = "";
    private List<String> operations = Lists.newArrayList();
    private final CountDownLatch flushLatch = new CountDownLatch(1);

    public List<String> getOperations()
    {
        return operations;
    }

    @Override
    public NodeStatus get()
    {
        return this;
    }

    @Override
    public JSONObject info()
    {
        return info;
    }

    public void setInfo(JSONObject info)
    {
        this.info = info;
    }

    @Override
    public boolean isJoined()
    {
        return false;
    }

    @Override
    public String getEndpoint()
    {
        return null;
    }

    @Override
    public int getExceptionCount()
    {
        return 0;
    }

    @Override
    public List<String> getLiveNodes()
    {
        return liveNodes;
    }

    public void setLiveNodes(List<String> liveNodes)
    {
        this.liveNodes = liveNodes;
    }

    @Override
    public List<String> getMovingNodes()
    {
        return movingNodes;
    }

    public void setMovingNodes(List<String> movingNodes)
    {
        this.movingNodes = movingNodes;
    }

    @Override
    public List<String> getJoiningNodes()
    {
        return joiningNodes;
    }

    public void setJoiningNodes(List<String> joiningNodes)
    {
        this.joiningNodes = joiningNodes;
    }

    @Override
    public List<String> getUnreachableNodes()
    {
        return unreachableNodes;
    }

    public void setUnreachableNodes(List<String> unreachableNodes)
    {
        this.unreachableNodes = unreachableNodes;
    }

    @Override
    public String getOperationMode()
    {
        return operationMode;
    }

    public void setOperationMode(String operationMode)
    {
        this.operationMode = operationMode;
    }

    @Override
    public String getGossipInfo()
    {
        return gossipInfo;
    }

    public void setGossipInfo(String gossipInfo)
    {
        this.gossipInfo = gossipInfo;
    }

    @Override
    public int getCompactionThroughput()
    {
        return 0;
    }

    @Override
    public void invalidateKeyCache()
    {
        operations.add("invalidateKeyCache");
    }

    @Override
    public void compact()
    {
        operations.add("compact");
    }

    @Override
    public void decommission()
    {
        operations.add("decommission");
    }

    @Override
    public void cleanup()
    {
        operations.add("cleanup");
    }

    @Override
    public void joinRing()
    {
        operations.add("joinRing");
    }

    @Override
    public void refresh(List<String> keyspaces)
    {
        operations.add("refresh," + Joiner.on(',').join(keyspaces));
    }

    @Override
    public void removeNode(String token)
    {
        operations.add("removeNode," + token);
    }

    @Override
    public void startThriftServer()
    {
        operations.add("startThriftServer");
    }

    @Override
    public void move(String token)
    {
        operations.add("move," + token);
    }

    public CountDownLatch getFlushLatch()
    {
        return flushLatch;
    }

    @Override
    public void flush() throws InterruptedException
    {
        operations.add("flush");
        flushLatch.countDown();
        Thread.currentThread().join();  // NOTE: Block forever
    }

    @Override
    public void repair(boolean sequential)
    {
        operations.add("repair," + sequential);
    }

    @Override
    public void invalidateRowCache()
    {
        operations.add("invalidateRowCache");
    }

    @Override
    public void stopGossiping()
    {
        operations.add("stopGossiping");
    }

    @Override
    public void startGossiping()
    {
        operations.add("startGossiping");
    }

    @Override
    public void stopThriftServer()
    {
        operations.add("stopThriftServer");
    }

    @Override
    public void drain()
    {
        operations.add("drain");
    }
}
