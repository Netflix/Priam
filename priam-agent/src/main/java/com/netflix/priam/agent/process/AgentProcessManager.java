package com.netflix.priam.agent.process;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.priam.agent.AgentConfiguration;
import com.netflix.priam.agent.NodeStatus;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Provider;
import java.io.Closeable;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * Manages running processes in the agent
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class AgentProcessManager implements Closeable
{
    private final AgentProcessMap processMap;
    private final AgentConfiguration configuration;
    private final Provider<NodeStatus> nodeToolProvider;
    private final ConcurrentMap<String, ProcessRecord> activeProcesses = Maps.newConcurrentMap();
    private final ExecutorService executorService;

    @GuardedBy("synchronized")
    private final Deque<ProcessRecord> completedProcesses = new LinkedBlockingDeque<ProcessRecord>();

    /**
     * @param processMap map from process name to process provider
     * @param configuration config
     * @param nodeToolProvider provider for the Node Tool operations
     */
    public AgentProcessManager(AgentProcessMap processMap, AgentConfiguration configuration, Provider<NodeStatus> nodeToolProvider)
    {
        this.processMap = processMap;
        this.configuration = configuration;
        this.nodeToolProvider = nodeToolProvider;
        executorService = Executors.newFixedThreadPool(configuration.getMaxProcessThreads(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AgentProcessManager-%d").build());
    }

    /**
     * List of currently executing processes
     *
     * @return processes
     */
    public List<ProcessRecord> getActiveProcesses()
    {
        return ImmutableList.copyOf(activeProcesses.values());
    }

    /**
     * List of completed processes
     *
     * @return processes
     */
    public List<ProcessRecord> getCompletedProcesses()
    {
        synchronized(completedProcesses)
        {
            return ImmutableList.copyOf(completedProcesses);
        }
    }

    /**
     * Start a process
     *
     * @param name name of the process to start (must exist in the process map)
     * @param id ID of the process. IDs must be unique. If there is already a process running with this ID this method will not start a new process.
     * @param arguments arguments for the processes
     * @return true if a new process was started
     * @throws Exception errors
     */
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
                validateArguments(process, arguments);

                Future<Void>    future = executorService.submit(new Executor(this, id, process, nodeToolProvider.get(), arguments));
                useProcessRecord.setExecutor(future);
                return true;
            }
            return false;
        }
    }

    /**
     * Attempt to stop the process with the given ID
     *
     * @param id ID of the process to stop
     * @return true if the process was found
     */
    public boolean stopProcess(String id)
    {
        final ProcessRecord processRecord = activeProcesses.get(id);
        if ( processRecord == null )
        {
            return false;
        }

        synchronized(processRecord)
        {
            processRecord.noteStopAttempt();
            Future<Void> executor = processRecord.getExecutor();
            if ( executor != null )
            {
                executor.cancel(true);
            }
        }
        return true;
    }

    /**
     * Stop all process and block until they complete or until time runs out
     *
     * @param timeout max time to wait for process completion
     * @param unit time unit
     * @return true if all processes terminated
     * @throws InterruptedException if interrupted
     */
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

    void removeProcess(String id)
    {
        final ProcessRecord processRecord = activeProcesses.remove(id);
        if ( processRecord == null )
        {
            return;
        }
        synchronized(processRecord)
        {
            processRecord.setEnd();
        }

        synchronized(completedProcesses)
        {
            try
            {
                while ( completedProcesses.size() >= configuration.getMaxCompletedProcesses() )
                {
                    completedProcesses.removeLast();
                }
            }
            catch ( NoSuchElementException ignore )
            {
                // ignore
            }
            completedProcesses.addFirst(processRecord);
        }
    }

    private void validateArguments(AgentProcess process, String[] arguments) throws IncorrectArgumentsException
    {
        ProcessMetaData metaData = process.getMetaData();
        if ( arguments.length < metaData.getMinArguments() )
        {
            throw new IncorrectArgumentsException("Expected at least " + metaData.getMinArguments() + " arguments but was only provided " + arguments.length);
        }
    }
}
