package com.netflix.priam.agent.task;

import com.netflix.astyanax.model.Column;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.process.AgentProcessManager;
import com.netflix.priam.scheduler.Task;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import static com.netflix.priam.agent.task.ProcessConstants.*;

public class ProcessTask extends Task
{
    private final AgentConfiguration    agentConfiguration;
    private final AgentProcessManager   processManager;

    private static final String     ROW_KEY = "com_commands";

    public ProcessTask(IConfiguration config, AgentConfiguration agentConfiguration, AgentProcessManager processManager)
    {
        super(config);
        this.agentConfiguration = agentConfiguration;
        this.processManager = processManager;
    }

    @Override
    public void execute() throws Exception
    {
        Column<String> result = agentConfiguration.getKeyspace()
            .prepareQuery(agentConfiguration.getColumnFamily())
            .getKey(ROW_KEY)
            .getColumn(agentConfiguration.getThisHostName())
            .execute()
            .getResult();

        JSONArray       tab = new JSONArray(result.getStringValue());
        for ( int i = 0; i < tab.length(); ++i )
        {
            JSONObject      commandObject = tab.getJSONObject(i);

            String          command = commandObject.getString(FIELD_COMMAND);
            String          id = commandObject.getString(FIELD_ID);

            if ( command.equals(COMMAND_START) )
            {
                String      name = commandObject.getString(FIELD_NAME);
                JSONArray   argumentsTab = commandObject.getJSONArray(FIELD_ARGUMENTS);
                String[]    arguments = new String[argumentsTab.length()];
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
        }
    }

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }
}
