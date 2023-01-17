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

package com.netflix.priam.stream;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.FifoQueue;
import org.junit.Assert;
import org.junit.Test;

public class StreamingTest {

    @Test
    public void testFifoAddAndRemove() {
        FifoQueue<Long> queue = new FifoQueue<>(10);
        for (long i = 0; i < 100; i++) queue.adjustAndAdd(i);
        Assert.assertEquals(10, queue.size());
        Assert.assertEquals(new Long(90), queue.first());
    }

    @Test
    public void testAbstractPath() {
        Injector injector = Guice.createInjector(new BRTestModule());
        IConfiguration conf = injector.getInstance(IConfiguration.class);
        InstanceIdentity factory = injector.getInstance(InstanceIdentity.class);
        String region = factory.getInstanceInfo().getRegion();

        FifoQueue<AbstractBackupPath> queue = new FifoQueue<>(10);
        for (int i = 10; i < 30; i++) {
            RemoteBackupPath path = new RemoteBackupPath(conf, factory);
            path.parseRemote(
                    "test_backup/"
                            + region
                            + "/fakecluster/123456/201108"
                            + i
                            + "0000"
                            + "/SNAP/ks1/cf2/f1"
                            + i
                            + ".db");
            queue.adjustAndAdd(path);
        }

        for (int i = 10; i < 30; i++) {
            RemoteBackupPath path = new RemoteBackupPath(conf, factory);
            path.parseRemote(
                    "test_backup/"
                            + region
                            + "/fakecluster/123456/201108"
                            + i
                            + "0000"
                            + "/SNAP/ks1/cf2/f2"
                            + i
                            + ".db");
            queue.adjustAndAdd(path);
        }

        for (int i = 10; i < 30; i++) {
            RemoteBackupPath path = new RemoteBackupPath(conf, factory);
            path.parseRemote(
                    "test_backup/"
                            + region
                            + "/fakecluster/123456/201108"
                            + i
                            + "0000"
                            + "/SNAP/ks1/cf2/f3"
                            + i
                            + ".db");
            queue.adjustAndAdd(path);
        }

        RemoteBackupPath path = new RemoteBackupPath(conf, factory);
        path.parseRemote(
                "test_backup/"
                        + region
                        + "/fakecluster/123456/201108290000"
                        + "/SNAP/ks1/cf2/f129.db");
        Assert.assertTrue(queue.contains(path));
        path.parseRemote(
                "test_backup/"
                        + region
                        + "/fakecluster/123456/201108290000"
                        + "/SNAP/ks1/cf2/f229.db");
        Assert.assertTrue(queue.contains(path));
        path.parseRemote(
                "test_backup/"
                        + region
                        + "/fakecluster/123456/201108290000"
                        + "/SNAP/ks1/cf2/f329.db");
        Assert.assertTrue(queue.contains(path));

        path.parseRemote(
                "test_backup/"
                        + region
                        + "/fakecluster/123456/201108260000/SNAP/ks1/cf2/f326.db To: cass/data/ks1/cf2/f326.db");
        Assert.assertEquals(path, queue.first());
    }

    @Test
    public void testIgnoreIndexFiles() {
        String[] testInputs =
                new String[] {
                    "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Digest.sha1",
                    "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Filter.db",
                    "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Data.db",
                    "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Statistics.db",
                    "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Filter.db",
                    "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Digest.sha1",
                    "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Statistics.db",
                    "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Data.db"
                };
    }
}
