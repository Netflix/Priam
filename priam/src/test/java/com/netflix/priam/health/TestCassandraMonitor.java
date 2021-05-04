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

package com.netflix.priam.health;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.merics.CassMonitorMetrics;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import mockit.*;
import org.apache.cassandra.tools.NodeProbe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by aagrawal on 7/18/17. */
public class TestCassandraMonitor {
    private static CassandraMonitor monitor;
    private static InstanceState instanceState;
    private static CassMonitorMetrics cassMonitorMetrics;
    private static IThriftChecker thriftChecker;

    private IConfiguration config;

    @Mocked private Process mockProcess;
    @Mocked private NodeProbe nodeProbe;
    @Mocked private ICassandraProcess cassProcess;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());
        config = injector.getInstance(IConfiguration.class);
        thriftChecker = injector.getInstance(ThriftChecker.class);
        if (instanceState == null) instanceState = injector.getInstance(InstanceState.class);
        if (cassMonitorMetrics == null)
            cassMonitorMetrics = injector.getInstance(CassMonitorMetrics.class);
        if (monitor == null)
            monitor =
                    new CassandraMonitor(
                            config, instanceState, cassProcess, cassMonitorMetrics, thriftChecker);
    }

    @Test
    public void testCassandraMonitor() throws Exception {
        monitor.execute();

        Assert.assertFalse(CassandraMonitor.hasCassadraStarted());

        CassandraMonitor.setIsCassadraStarted();
        Assert.assertTrue(CassandraMonitor.hasCassadraStarted());

        monitor.execute();
        Assert.assertFalse(CassandraMonitor.hasCassadraStarted());
    }

    @Test
    public void testNoAutoRemediation() throws Exception {
        new MockUp<JMXNodeTool>() {
            @Mock
            NodeProbe instance(IConfiguration config) {
                return nodeProbe;
            }
        };
        final InputStream mockOutput = new ByteArrayInputStream("a process".getBytes());
        new Expectations() {
            {
                mockProcess.getInputStream();
                result = mockOutput;
                nodeProbe.isGossipRunning();
                result = true;
                nodeProbe.isNativeTransportRunning();
                result = true;
                nodeProbe.isThriftServerRunning();
                result = true;
            }
        };
        // Mock out the ps call
        final Runtime r = Runtime.getRuntime();
        String[] cmd = {
            "/bin/sh",
            "-c",
            "ps -ef |grep -v -P \"\\sgrep\\s\" | grep " + config.getCassProcessName()
        };
        new Expectations(r) {
            {
                r.exec(cmd);
                result = mockProcess;
            }
        };
        instanceState.setShouldCassandraBeAlive(false);
        instanceState.setCassandraProcessAlive(false);

        monitor.execute();

        Assert.assertTrue(!instanceState.shouldCassandraBeAlive());
        Assert.assertTrue(instanceState.isCassandraProcessAlive());
        new Verifications() {
            {
                cassProcess.start(anyBoolean);
                times = 0;
            }
        };
    }

    @Test
    public void testAutoRemediationRateLimit() throws Exception {
        final InputStream mockOutput = new ByteArrayInputStream("".getBytes());
        instanceState.setShouldCassandraBeAlive(true);
        instanceState.markLastAttemptedStartTime();
        new Expectations() {
            {
                // 6 calls to execute should = 12 calls to getInputStream();
                mockProcess.getInputStream();
                result = mockOutput;
                times = 12;
                cassProcess.start(true);
                times = 2;
            }
        };
        // Mock out the ps call
        final Runtime r = Runtime.getRuntime();
        String[] cmd = {
            "/bin/sh",
            "-c",
            "ps -ef |grep -v -P \"\\sgrep\\s\" | grep " + config.getCassProcessName()
        };
        new Expectations(r) {
            {
                r.exec(cmd);
                result = mockProcess;
            }
        };
        // Sleep ahead to ensure we have permits in the rate limiter
        monitor.execute();
        Thread.sleep(1500);
        monitor.execute();
        monitor.execute();
        Thread.sleep(1500);
        monitor.execute();
        monitor.execute();
        monitor.execute();

        new Verifications() {};
    }
}
