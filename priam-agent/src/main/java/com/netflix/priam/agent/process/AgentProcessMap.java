package com.netflix.priam.agent.process;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.netflix.priam.agent.commands.*;
import javax.inject.Provider;
import java.util.Map;
import java.util.Set;

/**
 * Maps process names to process providers
 */
public class AgentProcessMap
{
    private final Map<String, Provider<? extends AgentProcess>> nameToProvider = Maps.newConcurrentMap();

    /**
     * Create an empty map
     */
    public AgentProcessMap()
    {
    }

    /**
     * Create a map with the given initial mappings
     *
     * @param nameToProvider mappings
     */
    public AgentProcessMap(Map<String, Provider<? extends AgentProcess>> nameToProvider)
    {
        this.nameToProvider.putAll(nameToProvider);
    }

    /**
     * Create a new process for the given process name
     *
     * @param name name of the process
     * @return the process
     * @throws NullPointerException if there is no mapping for the given name
     */
    public AgentProcess newProcess(String name)
    {
        Provider<? extends AgentProcess> provider = Preconditions.checkNotNull(nameToProvider.get(name), "No process found named: " + name);
        return provider.get();
    }

    /**
     * Return the names of the current mappings
     *
     * @return names
     */
    public Set<String> getNames()
    {
        return ImmutableSet.copyOf(nameToProvider.keySet());
    }

    /**
     * Return the meta data for the given process
     *
     * @param name process name
     * @return meta data or null
     */
    public ProcessMetaData getProcessMetaData(String name)
    {
        Provider<? extends AgentProcess> provider = nameToProvider.get(name);
        return (provider != null) ? provider.get().getMetaData() : null;
    }

    /**
     * Add a new mapping
     *
     * @param name process name
     * @param provider process provider
     */
    public void add(String name, Provider<? extends AgentProcess> provider)
    {
        nameToProvider.put(name, provider);
    }

    /**
     * Build a map with the default mappings. Can be used as the argument to {@link AgentProcessMap#AgentProcessMap(Map)}
     *
     * @return map
     */
    public static Map<String, Provider<? extends AgentProcess>> buildDefaultMap()
    {
        ImmutableMap.Builder<String, Provider<? extends AgentProcess>> builder = ImmutableMap.builder();

        builder.put("compact", SimpleProvider.of(CommandCompact.class));
        builder.put("cleanup", SimpleProvider.of(CommandCleanup.class));
        builder.put("flush", SimpleProvider.of(CommandFlush.class));
        builder.put("refresh", SimpleProvider.of(CommandRefresh.class));
        builder.put("repair", SimpleProvider.of(CommandRepair.class));
        builder.put("invalidate-key-cache", SimpleProvider.of(CommandInvalidateKeyCache.class));
        builder.put("invalidate-row-cache", SimpleProvider.of(CommandInvalidateRowCache.class));
        builder.put("drain", SimpleProvider.of(CommandDrain.class));
        builder.put("join-ring", SimpleProvider.of(CommandJoinRing.class));
        builder.put("decommission", SimpleProvider.of(CommandDecommission.class));
        builder.put("move", SimpleProvider.of(CommandMove.class));
        builder.put("remove-node", SimpleProvider.of(CommandRemoveNode.class));
        builder.put("stop-gossiping", SimpleProvider.of(CommandStopGossiping.class));
        builder.put("start-gossiping", SimpleProvider.of(CommandStartGossiping.class));
        builder.put("start-thrift", SimpleProvider.of(CommandStartThrift.class));
        builder.put("stop-thrift", SimpleProvider.of(CommandStopThrift.class));

        return builder.build();
    }
}
