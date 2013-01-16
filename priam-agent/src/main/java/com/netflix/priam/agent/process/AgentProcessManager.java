package com.netflix.priam.agent.process;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.priam.agent.AgentConfiguration;
import com.netflix.priam.agent.NodeStatus;
import javax.inject.Provider;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class AgentProcessManager implements Closeable
{
    private final AgentProcessMap processMap;
    private final Provider<NodeStatus> nodeToolProvider;
    private final ConcurrentMap<String, ProcessRecord> activeProcesses = Maps.newConcurrentMap();
    private final ExecutorService executorService;

    public AgentProcessManager(AgentProcessMap processMap, AgentConfiguration agentConfiguration, Provider<NodeStatus> nodeToolProvider)
    {
        this.processMap = processMap;
        this.nodeToolProvider = nodeToolProvider;
        executorService = Executors.newFixedThreadPool(agentConfiguration.getMaxProcessThreads(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AgentProcessManager-%d").build());
    }

    public List<ProcessRecord> getActiveProcesses()
    {
        return ImmutableList.copyOf(activeProcesses.values());
    }

    public boolean startProcess(String name, String id, String[] arguments) throws Exception
    {
        ProcessRecord newProcessRecord = new ProcessRecord(name, id, arguments);
        ProcessRecord oldProcessRecord = activeProcesses.putIfAbsent(id, newProcessRecord);
        final ProcessRecord useProcessRecord = (oldProcessRecord != null) ? oldProcessRecord : newProcessRecord;
        synchronized(useProcessRecord)
        {
            if ( useProcessRecord.getExecutor() == null )
            {
                AgentProcess    process = processMap.newProcess(name);
                Future<Void>    future = executorService.submit(new Executor(this, id, process, nodeToolProvider.get(), arguments));
                useProcessRecord.setExecutor(future);
                return true;
            }
            return false;
        }
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
            Future<Void> executor = processRecord.getExecutor();
            if ( executor != null )
            {
                executor.cancel(true);
            }
        }
    }

    public boolean closeAndWaitForCompletion(long timeout, TimeUnit unit) throws InterruptedException
    {
        close();
        return executorService.awaitTermination(timeout, unit);
    }

    @Override
    public void close()
    {
        executorService.shutdownNow();
    }
}
