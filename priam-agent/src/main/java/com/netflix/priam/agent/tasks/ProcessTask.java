package com.netflix.priam.agent.tasks;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.priam.agent.AgentConfiguration;
import com.netflix.priam.agent.process.AgentProcessManager;
import com.netflix.priam.agent.storage.Storage;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import static com.netflix.priam.agent.tasks.ProcessTaskConstants.*;

/**
 * Recurring task that starts/stops processes
 */
public class ProcessTask
{
    private final AgentConfiguration configuration;
    private final AgentProcessManager processManager;
    private final Storage storage;

    @VisibleForTesting
    public static final String ROW_KEY = "priam_agent_commands";

    public ProcessTask(AgentConfiguration configuration, AgentProcessManager processManager, Storage storage)
    {
        this.configuration = configuration;
        this.processManager = processManager;
        this.storage = storage;
    }

    public void execute() throws Exception
    {
        String ourValue = storage.getValue(configuration, ROW_KEY, configuration.getThisHostName());
        if ( ourValue == null )
        {
            return;
        }

        JSONArray tab = new JSONArray(ourValue);
        for ( int i = 0; i < tab.length(); ++i )
        {
            JSONObject commandObject = tab.getJSONObject(i);

            String command = commandObject.getString(FIELD_COMMAND);
            String id = commandObject.getString(FIELD_ID);

            if ( command.equals(COMMAND_START) )
            {
                String name = commandObject.getString(FIELD_NAME);
                JSONArray argumentsTab = commandObject.getJSONArray(FIELD_ARGUMENTS);
                String[] arguments = new String[argumentsTab.length()];
                for ( int j = 0; j < argumentsTab.length(); ++j )
                {
                    arguments[j] = argumentsTab.getString(j);
                }
                processManager.startProcess(name, id, arguments);
            }
            else if ( command.equals(COMMAND_STOP) )
            {
                processManager.stopProcess(id);
            }
            else
            {
                // TODO
            }
        }
    }
}
