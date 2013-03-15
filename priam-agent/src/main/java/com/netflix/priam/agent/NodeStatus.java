package com.netflix.priam.agent;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.IOException;
import java.util.List;

public interface NodeStatus
{
    public JSONObject info() throws JSONException;

    public boolean isJoined();

    public String getEndpoint();

    public int getExceptionCount();

    public List<String> getLiveNodes();

    public List<String> getMovingNodes();

    public List<String> getJoiningNodes();

    public List<String> getUnreachableNodes();

    public String getOperationMode();

    public String getGossipInfo();

    public int getCompactionThroughput();

    public void invalidateKeyCache() throws Exception;

    public void compact() throws Exception;

    public void decommission() throws Exception;

    public void cleanup() throws Exception;

    public void joinRing() throws IOException;

    public void refresh(List<String> keyspaces) throws Exception;

    public void removeNode(String token) throws Exception;

    public void startThriftServer() throws Exception;

    public void move(String token) throws Exception;

    public void flush() throws Exception;

    public void repair(boolean sequential) throws Exception;

    public void invalidateRowCache() throws Exception;

    public void stopGossiping() throws Exception;

    public void startGossiping() throws Exception;

    public void stopThriftServer() throws Exception;

    public void drain() throws Exception;
}
