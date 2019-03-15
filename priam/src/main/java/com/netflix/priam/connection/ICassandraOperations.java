/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.priam.connection;

import java.util.List;
import java.util.Map;

/** Created by aagrawal on 2/16/19. */
public interface ICassandraOperations {

    /**
     * This method neds to be synchronized. Context: During the transition phase to backup version
     * 2.0, we might be executing multiple snapshots at the same time. To avoid, unknown behavior by
     * Cassandra, it is wise to keep this method sync. Also, with backups being on CRON, we don't
     * know how often operator is taking snapshot.
     *
     * @param snapshotName Name of the snapshot on disk. This snapshotName should be UNIQUE among
     *     all the snapshots. Try to append UUID to snapshotName to ensure uniqueness. This is to
     *     ensure a) Snapshot fails if name are not unique. b) You might take snapshots which are
     *     not "part" of same snapshot. e.g. Any leftovers from previous operation. c) Once snapshot
     *     fails, this will clean the failed snapshot.
     * @throws Exception in case of error while taking a snapshot by Cassandra.
     */
    void takeSnapshot(final String snapshotName) throws Exception;

    /**
     * Clear the snapshot tag from disk.
     *
     * @param snapshotTag Name of the snapshot to be removed.
     * @throws Exception in case of error while clearing a snapshot.
     */
    void clearSnapshot(final String snapshotTag) throws Exception;

    /**
     * Get all the keyspaces existing on this node.
     *
     * @return List of keyspace names.
     * @throws Exception in case of reaching to JMX endpoint.
     */
    List<String> getKeyspaces() throws Exception;

    Map<String, List<String>> getColumnfamilies() throws Exception;

    void forceKeyspaceCompaction(String keyspaceName, String... columnfamilies) throws Exception;

    void forceKeyspaceFlush(String keyspaceName) throws Exception;

    List<Map<String, String>> gossipInfo() throws Exception;
}
