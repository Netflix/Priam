/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.defaultimpl;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * This class encapsulates interactions with Cassandra.
 * Created by aagrawal on 6/19/18.
 */
public class CassandraOperations {
    private static final Logger logger = LoggerFactory.getLogger(CassandraOperations.class);
    private IConfiguration configuration;

    @Inject
    CassandraOperations(IConfiguration configuration)
    {
        this.configuration = configuration;
    }

    /**
     * This method neds to be synchronized. Context: During the transition phase to backup version 2.0, we might be executing
     * multiple snapshots at the same time. To avoid, unknown behavior by Cassanddra, it is wise to keep this method sync.
     * Also, with backups being on CRON, we don't know how often operator is taking snapshot.
     * @param snapshotName Name of the snapshot on disk. This snapshotName should be UNIQUE among all the snapshots.
     *                     Try to append UUID to snapshotName to ensure uniqueness.
     *                     This is to ensure a) Snapshot fails if name are not unique.
     *                     b) You might take snapshots which are not "part" of same snapshot. e.g. Any leftovers from previous operation.
     *                     c) Once snapshot fails, this will clean the failed snapshot.
     * @throws Exception in case of error while taking a snapshot by Cassandra.
     */
    public synchronized void takeSnapshot(final String snapshotName) throws Exception {
        //Retry max of 6 times with 10 second in between (for one minute). This is to ensure that we overcome any temporary glitch.
        //Note that operation MAY fail if cassandra successfully took the snapshot of certain columnfamily(ies) and we try to create snapshot with
        //same name. It is a good practice to call clearSnapshot after this operation fails, to ensure we don't leave
        //any left overs.
        //Example scenario: Change of file permissions by manual intervention and C* unable to take snapshot of one CF.
        try {
            new RetryableCallable<Void>(6, 10000) {
                public Void retriableCall() throws Exception {
                    JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                    nodetool.takeSnapshot(snapshotName, null);
                    return null;
                }
            }.call();
        }catch (Exception e){
            logger.error("Error while taking snapshot {}. Asking Cassandra to clear snapshot to avoid accumulation of snapshots.", snapshotName);
            clearSnapshot(snapshotName);
            throw e;
        }
    }

    /**
     * Clear the snapshot tag from disk.
     * @param snapshotTag Name of the snapshot to be removed.
     * @throws Exception in case of error while clearing a snapshot.
     */
    public void clearSnapshot(final String snapshotTag) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                nodetool.clearSnapshot(snapshotTag);
                return null;
            }
        }.call();
    }
}
