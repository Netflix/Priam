/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.identity;

import com.google.common.collect.ImmutableSet;
import com.google.inject.ImplementedBy;
import com.netflix.priam.aws.SDBInstanceFactory;
import java.util.Map;

/**
 * Interface for managing Cassandra instance data. Provides functionality to register, update,
 * delete or list instances from the registry
 */
@ImplementedBy(SDBInstanceFactory.class)
public interface IPriamInstanceFactory {
    /**
     * Return a list of all Cassandra server nodes registered.
     *
     * @param appName the cluster name
     * @return a list of all nodes in {@code appName}
     */
    ImmutableSet<PriamInstance> getAllIds(String appName);

    /**
     * Return the Cassandra server node with the given {@code id}.
     *
     * @param appName the cluster name
     * @param id the node id
     * @return the node with the given {@code id}, or {@code null} if none found
     */
    PriamInstance getInstance(String appName, String dc, int id);

    /**
     * Create/Register an instance of the server with its info.
     *
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
    PriamInstance create(
            String app,
            int id,
            String instanceID,
            String hostname,
            String ip,
            String rac,
            Map<String, Object> volumes,
            String token);

    /**
     * Delete the server node from the registry
     *
     * @param inst the node to delete
     */
    void delete(PriamInstance inst);

    /**
     * Update the details of the server node in registry
     *
     * @param orig the values that should exist in the database for the update to succeed
     * @param inst the new values
     */
    void update(PriamInstance orig, PriamInstance inst);
}
