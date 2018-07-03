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

import javax.inject.Inject;

/**
 * This class encapsulates interactions with Cassandra.
 * Created by aagrawal on 6/19/18.
 */
public class CassandraOperations {

    private IConfiguration configuration;

    @Inject
    CassandraOperations(IConfiguration configuration)
    {
        this.configuration = configuration;
    }

    public void takeSnapshot(final String snapshotName) throws Exception {
        new RetryableCallable<Void>() {
            public Void retriableCall() throws Exception {
                JMXNodeTool nodetool = JMXNodeTool.instance(configuration);
                nodetool.takeSnapshot(snapshotName, null);
                return null;
            }
        }.call();
    }

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
