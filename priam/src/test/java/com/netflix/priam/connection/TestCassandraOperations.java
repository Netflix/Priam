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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mchange.io.FileUtils;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.CassandraMonitor;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.cassandra.tools.NodeProbe;
import org.junit.*;
import org.junit.rules.TestName;

/** Created by aagrawal on 3/1/19. */
public class TestCassandraOperations {
    private final String gossipInfo1 = "src/test/resources/gossipInfoSample_1.txt";
    @Mocked private NodeProbe nodeProbe;
    @Mocked private JMXNodeTool jmxNodeTool;
    private FakeConfiguration config;
    private static CassandraOperations cassandraOperations;
    private static final String BASE_DIR = "base";
    private static final String RESTORE_DIR = "restore";
    private static final String DATA_DIR = "data";
    private static final String KS = "ks";
    private static final String TB = "tb";
    private static final String SI = "si";
    private static final String DATAFILE = "datafile";
    private static final String SI_DATAFILE = "si_datafile";

    @Rule public TestName name = new TestName();

    @BeforeClass
    public static void prepBeforeAllTests() {
        deleteBaseDirectoryForAllTests();
    }

    @Before
    public void prepareTest() throws IOException {
        new MockUp<JMXNodeTool>() {
            @Mock
            NodeProbe instance(IConfiguration config) {
                return nodeProbe;
            }
        };
        Injector injector = Guice.createInjector(new BRTestModule());
        cassandraOperations = injector.getInstance(CassandraOperations.class);
        Paths.get(getRestoreDir(), KS, TB, SI).toFile().mkdirs();
        Files.touch(Paths.get(getRestoreDir(), KS, TB, DATAFILE).toFile());
        Files.touch(Paths.get(getRestoreDir(), KS, TB, SI, SI_DATAFILE).toFile());
        config = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        config.setDataFileLocation(getDataDir());
        CassandraMonitor.setIsCassandraStarted(false);
    }

    @After
    public void cleanup() {
        org.apache.commons.io.FileUtils.deleteQuietly(Paths.get(getRestoreDir()).toFile());
        org.apache.commons.io.FileUtils.deleteQuietly(Paths.get(getDataDir()).toFile());
        config.setDataFileLocation(null);
    }

    @AfterClass
    public static void cleanupAfterAllTests() {
        deleteBaseDirectoryForAllTests();
    }

    @Test
    public void testGossipInfo() throws Exception {
        String gossipInfoFromNodetool = FileUtils.getContentsAsString(new File(gossipInfo1));
        new Expectations() {
            {
                nodeProbe.getGossipInfo(false);
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

    @Test
    public void testRestoreViaMove_dataDirectoryIsCreated() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getDataDir()).toFile().exists());
    }

    @Test
    public void testRestoreViaMove_keyspaceDirectoryIsCreated() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getDataDir(), KS).toFile().exists());
    }

    @Test
    public void testRestoreViaMove_tableDirectoryIsCreated() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getDataDir(), KS, TB).toFile().exists());
    }

    @Test
    public void testRestoreViaMove_secondaryIndexDirectoryIsCreated() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getDataDir(), KS, TB, SI).toFile().exists());
    }

    @Test
    public void testRestoreViaMove_dataFileIsMoved() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getDataDir(), KS, TB, DATAFILE).toFile().exists());
    }

    @Test
    public void testRestoreViaMove_dataFileIsRemovedFromOrigin() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getRestoreDir(), KS, TB, DATAFILE).toFile().exists()).isFalse();
    }

    @Test
    public void testRestoreViaMove_secondaryIndexFileIsMoved() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getDataDir(), KS, TB, SI, SI_DATAFILE).toFile().exists());
    }

    @Test
    public void testRestoreViaMove_secondaryIndexFileIsRemovedFromOrigin() throws IOException {
        cassandraOperations.importAll(getRestoreDir());
        Truth.assertThat(Paths.get(getRestoreDir(), KS, TB, SI, SI_DATAFILE).toFile().exists())
                .isFalse();
    }

    @Test
    public void testRestoreViaImport() throws IOException {
        CassandraMonitor.setIsCassandraStarted(true);
        new Expectations() {
            {
                nodeProbe.importNewSSTables(
                        KS,
                        TB,
                        ImmutableSet.of(Paths.get(getRestoreDir(), KS, TB).toString()),
                        false /* resetLevel */,
                        false /* clearRepaired */,
                        true /* verifySSTables */,
                        true /* verifyTokens */,
                        true /* invalidateCaches */,
                        false /* extendedVerify */,
                        false /* copyData */);
                result = new ArrayList<>();
            }
        };
        cassandraOperations.importAll(getRestoreDir());
    }

    private static void deleteBaseDirectoryForAllTests() {
        org.apache.commons.io.FileUtils.deleteQuietly(Paths.get(BASE_DIR).toFile());
    }

    private String getRestoreDir() {
        return BASE_DIR + "/" + name.getMethodName() + RESTORE_DIR;
    }

    private String getDataDir() {
        return BASE_DIR + "/" + name.getMethodName() + DATA_DIR;
    }
}
