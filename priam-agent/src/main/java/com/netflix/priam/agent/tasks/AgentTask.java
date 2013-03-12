package com.netflix.priam.agent.tasks;

import com.google.common.base.Joiner;
import com.netflix.priam.agent.AgentConfiguration;
import com.netflix.priam.agent.NodeStatus;
import com.netflix.priam.agent.process.AgentProcessManager;
import com.netflix.priam.agent.process.ProcessRecord;
import com.netflix.priam.agent.storage.Storage;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONArray;
import javax.inject.Provider;

/**
 * Recurring task that writes out state info
 */
public class AgentTask
{
    private final AgentConfiguration configuration;
    private final AgentProcessManager processManager;
    private final Provider<NodeStatus> nodeToolProvider;
    private final Storage storage;

    private static final String ROW_KEY = "priam_agent_instances";
    private static final int VERSION = 0x00010001;

    public AgentTask(AgentConfiguration configuration, AgentProcessManager processManager, Provider<NodeStatus> nodeToolProvider, Storage storage)
    {
        this.configuration = configuration;
        this.processManager = processManager;
        this.nodeToolProvider = nodeToolProvider;
        this.storage = storage;
    }

    public void execute() throws Exception
    {
        JSONObject json = new JSONObject();

        NodeStatus nodeTool = nodeToolProvider.get();
        json.put("version", VERSION);
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
        json.put("active_processes", getActiveProcesses());
        json.put("completed_processes", getCompletedProcesses());

        storage.setValue(configuration, ROW_KEY, configuration.getThisHostName(), json.toString());
    }

    private JSONArray getCompletedProcesses() throws JSONException
    {
        JSONArray tab = new JSONArray();
        for ( ProcessRecord processRecord : processManager.getCompletedProcesses() )
        {
            JSONObject json = new JSONObject();
            addProcessCommon(processRecord, json);
            json.put("end_time_ms", processRecord.getEndTimeMs());
        }

        return tab;
    }

    private JSONArray getActiveProcesses() throws JSONException
    {
        JSONArray tab = new JSONArray();
        for ( ProcessRecord processRecord : processManager.getActiveProcesses() )
        {
            JSONObject json = new JSONObject();
            addProcessCommon(processRecord, json);
            json.put("stop_attempt_ms", processRecord.getStopAttemptMs());
        }

        return tab;
    }

    private void addProcessCommon(ProcessRecord processRecord, JSONObject json) throws JSONException
    {
        json.put("name", processRecord.getName());
        json.put("id", processRecord.getId());
        json.put("start_time_ms", processRecord.getStartTimeMs());
        json.put("elapsed_time_ms", System.currentTimeMillis() - processRecord.getStartTimeMs());
        json.put("arguments", Joiner.on(", ").join(processRecord.getArguments()));
    }
}
