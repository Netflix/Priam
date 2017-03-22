/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.cluster.management;

import org.apache.cassandra.db.Keyspace;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXConnectorMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility to flush 1:8 Keyspaces from memtable to disk
 *
 * Created by vinhn on 10/12/16.
 */
public class Flush implements IClusterManagement<String> {
    private static final Logger logger = LoggerFactory.getLogger(Flush.class);

    private final IConfiguration config;
    private final JMXConnectorMgr jmxConnectorMgr;
    private List<String> keyspaces = new ArrayList<String>();

    public Flush(IConfiguration config, JMXConnectorMgr jmxConnectorMgr) {
        this.config = config;
        this.jmxConnectorMgr = jmxConnectorMgr;
    }

    @Override
    /*
     * @return the keyspace(s) flushed.  List can be empty but never null.
     */
    public List<String> execute() throws Exception {
        List<String> flushed = new ArrayList<String>();

        //== fetch keyspaces
        deriveKeyspaces();
        if (this.keyspaces == null || this.keyspaces.isEmpty()) {
            logger.warn("NO op on requested \"flush\" as there are no keyspaces.");
            return flushed;
        }

        //If flush is for certain keyspaces, validate keyspace exist
        for (String keyspace : keyspaces) {
            if (!this.jmxConnectorMgr.getKeyspaces().contains(keyspace)) {
                throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }
        }

        for (String keyspace : keyspaces) { //flush each keyspace with the CFs.
            if (Keyspace.SYSTEM_KS.equals(keyspace)) //no need to flush system keyspaces.
                continue;

            try {
                this.jmxConnectorMgr.forceKeyspaceFlush(keyspace, new String[0]);
                flushed.add(keyspace);
            } catch (Exception e) {
                throw new RuntimeException("Exception flush keyspace: " + keyspace, e);
            }
        }

        return flushed;
    }

    /*
    Derive keyspace(s) to flush in the following order:  explicit list provided by caller, property, or all keyspaces.
     */
    private void deriveKeyspaces() {
        //== get value from property
        String raw = this.config.getFlushKeyspaces();
        if (raw != null && !raw.isEmpty() ) {
            String k[] = raw.split(",");
            for (int i=0; i < k.length; i++ ) {
                this.keyspaces.add(i, k[i]);
            }

            return;
        }

        //== no override via FP, default to all keyspaces
        this.keyspaces = this.jmxConnectorMgr.getKeyspaces();
        return;
    }
}
