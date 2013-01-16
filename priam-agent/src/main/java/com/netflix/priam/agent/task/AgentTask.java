package com.netflix.priam.agent.task;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.process.AgentProcess;
import com.netflix.priam.agent.process.AgentProcessManager;
import com.netflix.priam.agent.process.ProcessRecord;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONArray;

public class AgentTask extends Task
{
    private final AgentConfiguration    agentConfiguration;
    private final AgentProcessManager   processManager;

    private static final String     ROW_KEY = "com_instances";

    public AgentTask(IConfiguration configuration, AgentConfiguration agentConfiguration, AgentProcessManager processManager)
    {
        super(configuration);
        this.agentConfiguration = agentConfiguration;
        this.processManager = processManager;
    }

    @Override
    public void execute() throws Exception
    {
        new RetryableCallable<Void>()
        {
            public Void retriableCall() throws Exception
            {
                JSONObject      json = new JSONObject();

                JMXNodeTool     nodeTool = JMXNodeTool.instance(config);
                json.put("current_time_ms", System.currentTimeMillis());
                json.put("info", nodeTool.info());
                json.put("is_joined", nodeTool.isJoined());
                json.put("endpoint", nodeTool.getEndpoint());
                json.put("exception_count", nodeTool.getExceptionCount());
                json.put("live_nodes", nodeTool.getLiveNodes());
                json.put("moving_nodes", nodeTool.getMovingNodes());
                json.put("joining_nodes", nodeTool.getJoiningNodes());
                json.put("unreachable_nodes", nodeTool.getUnreachableNodes());
                json.put("operation_mode", nodeTool.getOperationMode());
                json.put("gossip_info", nodeTool.getGossipInfo());
                json.put("compaction_throughput", nodeTool.getCompactionThroughput());
                json.put("agent-processes", getProcesses());

                agentConfiguration.getKeyspace()
                    .prepareColumnMutation(agentConfiguration.getColumnFamily(), ROW_KEY, agentConfiguration.getThisHostName())
                    .putValue(json.toString(), agentConfiguration.getCassandraTtl())
                    .execute();

                return null;
            }
        }.call();
    }

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }

    private JSONArray getProcesses() throws JSONException
    {
        JSONArray       tab = new JSONArray();
        for ( ProcessRecord processRecord : processManager.getActiveProcesses() )
        {
            AgentProcess    process = processRecord.getProcess();

            JSONObject      json = new JSONObject();
            json.put("name", processRecord.getName());
            json.put("id", processRecord.getId());
            json.put("start_time_ms", processRecord.getStartTimeMs());
        }

        return tab;
    }
}
