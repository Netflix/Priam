package com.netflix.priam.agent.process;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.agent.task.AgentConfiguration;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class AgentProcessManager implements Closeable
{
    private final AgentProcessMap processMap;
    private final IConfiguration configuration;
    private final ConcurrentMap<String, ProcessRecord> activeProcesses = Maps.newConcurrentMap();
    private final ExecutorService executorService;

    public AgentProcessManager(AgentProcessMap processMap, IConfiguration configuration, AgentConfiguration agentConfiguration)
    {
        this.processMap = processMap;
        this.configuration = configuration;
        executorService = Executors.newFixedThreadPool(agentConfiguration.getMaxProcessThreads(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AgentProcessManager-%d").build());
    }

    public List<ProcessRecord> getActiveProcesses()
    {
        return ImmutableList.copyOf(activeProcesses.values());
    }

    public boolean startProcess(String name, String id, String[] arguments) throws Exception
    {
        return internalStartProcess(name, id, arguments, false);
    }

    public void forceStartProcess(String name, String id, String[] arguments) throws Exception
    {
        internalStartProcess(name, id, arguments, true);
    }

    public void stopProcess(String id)
    {
        final ProcessRecord processRecord = activeProcesses.remove(id);
        if ( processRecord == null )
        {
            // TODO
            return;
        }
        synchronized(processRecord)
        {
            Executor    executor = processRecord.getExecutor();
            if ( executor != null )
            {
                executor.interruptTask();
            }
        }
    }

    @Override
    public void close()
    {
        executorService.shutdownNow();
    }

    private boolean internalStartProcess(String name, String id, String[] arguments, boolean force) throws Exception
    {
        ProcessRecord newProcessRecord = new ProcessRecord(name, id);
        ProcessRecord oldProcessRecord = activeProcesses.putIfAbsent(id, newProcessRecord);
        final ProcessRecord useProcessRecord = (oldProcessRecord != null) ? oldProcessRecord : newProcessRecord;
        synchronized(useProcessRecord)
        {
            Executor executor = useProcessRecord.getExecutor();

            if ( force && (executor != null) )
            {
                try
                {
                    executor.interruptTask();
                }
                finally
                {
                    useProcessRecord.setProcess(null, null);
                    executor = null;
                }
            }

            if ( executor == null )
            {
                AgentProcess    process = processMap.newProcess(name);
                executor = new Executor(process, configuration, arguments);

                executorService.submit(executor);
                useProcessRecord.setProcess(process, executor);
                return true;
            }
            return false;
        }
    }
}
