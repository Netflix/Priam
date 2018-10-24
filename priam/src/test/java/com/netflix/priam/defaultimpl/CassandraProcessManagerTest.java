/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.priam.defaultimpl;

import com.google.inject.Guice;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.merics.CassMonitorMetrics;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CassandraProcessManagerTest {
    private CassandraProcessManager cpm;

    @Before
    public void setup() {
        IConfiguration config = new FakeConfiguration("test_cluster");
        InstanceState instanceState =
                Guice.createInjector(new BRTestModule()).getInstance(InstanceState.class);
        CassMonitorMetrics cassMonitorMetrics =
                Guice.createInjector(new BRTestModule()).getInstance(CassMonitorMetrics.class);

        cpm = new CassandraProcessManager(config, instanceState, cassMonitorMetrics);
    }

    @Test
    public void logProcessOutput_BadApp() throws IOException, InterruptedException {
        Process p = null;
        try {
            p = new ProcessBuilder("ls", "/tmppppp").start();
            int exitValue = p.waitFor();
            Assert.assertTrue(0 != exitValue);
            cpm.logProcessOutput(p);
        } catch (IOException ioe) {
            if (p != null) cpm.logProcessOutput(p);
        }
    }

    /** note: this will succeed on a *nix machine, unclear about anything else... */
    @Test
    public void logProcessOutput_GoodApp() throws IOException, InterruptedException {
        Process p = new ProcessBuilder("true").start();
        int exitValue = p.waitFor();
        Assert.assertEquals(0, exitValue);
        cpm.logProcessOutput(p);
    }
}
