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
package com.netflix.priam.utils;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Represents a connection to remote JMX mbean server.  This object differs from JMXNodeTool as it is meant for short
 * lived connection to remote mbean server.
 *
 * Created by vinhn on 10/11/16.
 */
public class JMXConnectorMgr extends NodeProbe {
    private static final Logger logger = LoggerFactory.getLogger(JMXConnectorMgr.class);

    /*
     * create a connection to remote mbean server and get proxy to various mbeans
     * @throws exception if unable to create the connection, e.g. Cassandra process not running.
     */
    public JMXConnectorMgr(IConfiguration config) throws IOException, InterruptedException {
        super("localhost", config.getJmxPort());
    }

    @Override
    /*
    close the connection to remote mbean server
     */
    public void close() throws IOException {
        super.close(); //close the connection to remote mbean server
    }

}
