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

package com.netflix.priam.resources;

import static org.junit.Assert.assertEquals;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.*;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.ICassandraProcess;
import com.netflix.priam.health.InstanceState;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.restore.Restore;
import com.netflix.priam.tuner.ICassandraTuner;
import com.netflix.priam.utils.DateUtil;
import java.util.Date;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMockit.class)
public class BackupServletTest {
    private @Mocked PriamServer priamServer;
    private IConfiguration config;
    private @Mocked IBackupFileSystem bkpFs;
    private @Mocked Restore restoreObj;
    private @Mocked Provider<AbstractBackupPath> pathProvider;
    private @Mocked ICassandraTuner tuner;
    private @Mocked SnapshotBackup snapshotBackup;
    private @Mocked IPriamInstanceFactory factory;
    private @Mocked ICassandraProcess cassProcess;
    private @Mocked BackupStatusMgr bkupStatusMgr;
    private BackupServlet resource;
    private RestoreServlet restoreResource;
    private BackupVerification backupVerification;
    private InstanceInfo instanceInfo;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());
        config = injector.getInstance(IConfiguration.class);
        InstanceState instanceState = injector.getInstance(InstanceState.class);
        instanceInfo = injector.getInstance(InstanceInfo.class);
        resource =
                new BackupServlet(config, bkpFs, snapshotBackup, bkupStatusMgr, backupVerification);
        restoreResource = new RestoreServlet(restoreObj, instanceState);
    }

    @Test
    public void backup() throws Exception {
        new Expectations() {
            {
                snapshotBackup.execute();
            }
        };

        Response response = resource.backup();
        assertEquals(200, response.getStatus());
        assertEquals("[\"ok\"]", response.getEntity());
        assertEquals(
                MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void restore_minimal() throws Exception {
        final String dateRange = null;
        final String oldRegion = "us-east-1";
        new Expectations() {
            {
                instanceInfo.getRegion();
                result = oldRegion;

                restoreObj.restore((Date) any, (Date) any); // TODO: test default value
            }
        };

        expectCassandraStartup();

        Response response = restoreResource.restore(dateRange);
        assertEquals(200, response.getStatus());
        assertEquals("[\"ok\"]", response.getEntity());
        assertEquals(
                MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    @Test
    public void restore_withDateRange() throws Exception {
        final String dateRange = "201101010000,201112312359";

        new Expectations() {

            {
                DateUtil.getDate(dateRange.split(",")[0]);
                result = new DateTime(2011, 1, 1, 0, 0).toDate();
                times = 1;
                DateUtil.getDate(dateRange.split(",")[1]);
                result = new DateTime(2011, 12, 31, 23, 59).toDate();
                times = 1;
                restoreObj.restore(
                        DateUtil.getDate(dateRange.split(",")[0]),
                        DateUtil.getDate(dateRange.split(",")[1]));
            }
        };

        expectCassandraStartup();

        Response response = restoreResource.restore(dateRange);
        assertEquals(200, response.getStatus());
        assertEquals("[\"ok\"]", response.getEntity());
        assertEquals(
                MediaType.APPLICATION_JSON_TYPE, response.getMetadata().get("Content-Type").get(0));
    }

    // TODO: create CassandraController interface and inject, instead of static util method
    private void expectCassandraStartup() {
        new Expectations() {
            {
                config.getCassStartupScript();
                result = "/usr/bin/false";
                config.getHeapNewSize();
                result = "2G";
                config.getHeapSize();
                result = "8G";
                config.getDataFileLocation();
                result = "/var/lib/cassandra/data";
                config.getCommitLogLocation();
                result = "/var/lib/cassandra/commitlog";
                config.getBackupLocation();
                result = "backup";
                config.getCacheLocation();
                result = "/var/lib/cassandra/saved_caches";
                config.getJmxPort();
                result = 7199;
                config.getMaxDirectMemory();
                result = "50G";
            }
        };
    }
}
