/*
 * Copyright 2018 Netflix, Inc.
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
 */

package com.netflix.priam.restore;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.TestModule;
import com.netflix.priam.config.IConfiguration;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPostRestoreHook {

    @Before
    @After
    public void setup() {
        Injector inject = Guice.createInjector(new TestModule());
        IConfiguration configuration = inject.getInstance(IConfiguration.class);

        // ensure heartbeat and done files are not present
        File heartBeatFile = new File(configuration.getPostRestoreHookHeartbeatFileName());
        if (heartBeatFile.exists()) {
            heartBeatFile.delete();
        }

        File doneFile = new File(configuration.getPostRestoreHookDoneFileName());
        if (doneFile.exists()) {
            doneFile.delete();
        }
    }

    @Test
    /*
     Test to validate hasValidParameters. Expected to pass since none of the parameters in
     FakeConfiguration are blank
    */
    public void testPostRestoreHookValidParameters() {
        Injector inject = Guice.createInjector(new TestModule());
        IPostRestoreHook postRestoreHook = inject.getInstance(IPostRestoreHook.class);
        Assert.assertTrue(postRestoreHook.hasValidParameters());
    }

    @Test
    /*
     Test to validate execute method. This is a happy path since heart beat file is emited as soon
     as test case starts, and postrestorehook completes execution once the child process completes
     execution. Test fails in case of any exception.
    */
    public void testPostRestoreHookExecuteHappyPath() throws Exception {
        Injector inject = Guice.createInjector(new TestModule());
        IPostRestoreHook postRestoreHook = inject.getInstance(IPostRestoreHook.class);
        IConfiguration configuration = inject.getInstance(IConfiguration.class);
        startHeartBeatThreadWithDelay(
                0,
                configuration.getPostRestoreHookHeartbeatFileName(),
                configuration.getPostRestoreHookDoneFileName());
        postRestoreHook.execute();
    }

    @Test
    /*
     Test to validate execute method. This is a variant of above method, where heartbeat is
     produced after an initial delay. This delay causes PostRestoreHook to terminate the child
     process since there is no heartbeat multiple times, and eventually once the heartbeat starts,
     PostRestoreHook waits for the child process to complete execution. Test fails in case of any
     exception.
    */
    public void testPostRestoreHookExecuteHeartBeatDelay() throws Exception {
        Injector inject = Guice.createInjector(new TestModule());
        IPostRestoreHook postRestoreHook = inject.getInstance(IPostRestoreHook.class);
        IConfiguration configuration = inject.getInstance(IConfiguration.class);
        startHeartBeatThreadWithDelay(
                1000,
                configuration.getPostRestoreHookHeartbeatFileName(),
                configuration.getPostRestoreHookDoneFileName());
        postRestoreHook.execute();
    }

    /**
     * Starts a thread to emit heartbeat and finish with a done file.
     *
     * @param delayInMs any start up delay if needed
     * @param heartBeatfileName name of the heart beat file
     * @param doneFileName name of the done file
     */
    private void startHeartBeatThreadWithDelay(
            long delayInMs, String heartBeatfileName, String doneFileName) {
        Thread heartBeatEmitThread =
                new Thread(
                        () -> {
                            File heartBeatFile = new File(heartBeatfileName);
                            try {
                                // add a delay to heartbeat
                                Thread.sleep(delayInMs);
                                if (!heartBeatFile.exists() && !heartBeatFile.createNewFile()) {
                                    Assert.fail("Unable to create heartbeat file");
                                }
                                for (int i = 0; i < 10; i++) {
                                    FileUtils.touch(heartBeatFile);
                                    Thread.sleep(1000);
                                }

                                File doneFile = new File(doneFileName);
                                doneFile.createNewFile();
                            } catch (Exception ex) {
                                Assert.fail(ex.getMessage());
                            }
                        });

        heartBeatEmitThread.start();
    }
}
