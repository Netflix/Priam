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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mchange.io.FileUtils;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import java.util.List;
import java.util.Map;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.cassandra.tools.NodeProbe;
import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 3/1/19. */
public class TestCassandraOperations {
    private final String gossipInfo1 = "src/test/resources/gossipInfoSample_1.txt";
    @Mocked private NodeProbe nodeProbe;
    @Mocked private JMXNodeTool jmxNodeTool;
    private static CassandraOperations cassandraOperations;

    public TestCassandraOperations() {
        new MockUp<JMXNodeTool>() {
            @Mock
            NodeProbe instance(IConfiguration config) {
                return nodeProbe;
            }
        };
        Injector injector = Guice.createInjector(new BRTestModule());
        if (cassandraOperations == null)
            cassandraOperations = injector.getInstance(CassandraOperations.class);
    }

    @Test
    public void testGossipInfo() throws Exception {

        String gossipInfoFromNodetool = FileUtils.getContentsAsString(new File(gossipInfo1));
        new Expectations() {
            {
                nodeProbe.getGossipInfo();
                result = gossipInfoFromNodetool;
                nodeProbe.getTokens("127.0.0.1");
                result = "123,234";
            }
        };
        List<Map<String, String>> gossipInfoList = cassandraOperations.gossipInfo();
        System.out.println(gossipInfoList);
        Assert.assertEquals(7, gossipInfoList.size());
        gossipInfoList
                .stream()
                .forEach(
                        gossipInfo -> {
                            Assert.assertEquals("us-east", gossipInfo.get("DC"));
                            Assert.assertNotNull(gossipInfo.get("PUBLIC_IP"));
                            Assert.assertEquals("1565153", gossipInfo.get("HEARTBEAT"));
                            if (gossipInfo.get("STATUS").equalsIgnoreCase("NORMAL"))
                                Assert.assertNotNull(gossipInfo.get("TOKENS"));
                            if (gossipInfo.get("PUBLIC_IP").equalsIgnoreCase("127.0.0.1"))
                                Assert.assertEquals("[123,234]", gossipInfo.get("TOKENS"));
                        });
    }
}
