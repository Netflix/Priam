package com.netflix.priam.identity;

import java.util.List;
import java.util.Map;

/**
 *  Interface for managing Cassandra instance data. Provides functionality
 *  to register, update, delete or list instances from the registry 
 *
 */
public interface IPriamInstanceFactory
{
    /**
     * Return a list of all Cassandra server nodes registered.
     * @param appName the cluster name
     * @return a list of all nodes in {@code appName}
     */
    public List<PriamInstance> getAllIds(String appName);

    /**
     * Return the Cassandra server node with the given {@code id}.
     * @param appName the cluster name
     * @param id the node id
     * @return the node with the given {@code id}, or {@code null} if none found
     */
    public PriamInstance getInstance(String appName, int id);

    /**
     * Create/Register an instance of the server with its info.
     * @param app
     * @param id
     * @param instanceID
     * @param hostname
     * @param ip
     * @param rac
     * @param volumes
     * @param token
     * @return the new node
     */
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token);

    /**
     * Delete the server node from the registry
     * @param inst the node to delete
     */
    public void delete(PriamInstance inst);

    /**
     * Update the details of the server node in registry
     * @param inst the node to update
     */
    public void update(PriamInstance inst);

    /**
     * Sort the list by instance ID
     * @param return_ the list of nodes to sort
     */
    public void sort(List<PriamInstance> return_);

    /**
     * Attach volumes if required
     * @param instance
     * @param mountPath
     * @param device
     */
    public void attachVolumes(PriamInstance instance, String mountPath, String device);
}