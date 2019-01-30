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
package com.netflix.priam.health;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test InstanceState Created by aagrawal on 9/22/17. */
public class TestInstanceStatus {
    private TestInstanceState testInstanceState;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());
        InstanceState instanceState = injector.getInstance(InstanceState.class);
        testInstanceState = new TestInstanceState(instanceState);
    }

    @Test
    public void testHealth() {
        // Verify good health.
        Assert.assertTrue(
                testInstanceState
                        .setParams(false, true, true, true, true, true, true, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(false, true, true, true, true, false, true, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(false, true, true, true, false, true, true, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(true, false, true, true, false, true, true, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(true, true, false, true, true, true, true, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(true, true, true, false, true, true, true, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(true, true, true, true, true, true, false, true)
                        .isHealthy());
        Assert.assertTrue(
                testInstanceState
                        .setParams(true, true, true, true, false, false, true, true)
                        .isHealthy());

        // Negative health case scenarios.
        Assert.assertFalse(
                testInstanceState
                        .setParams(false, false, true, true, false, true, true, true)
                        .isHealthy());
        Assert.assertFalse(
                testInstanceState
                        .setParams(false, true, false, true, true, true, true, true)
                        .isHealthy());
        Assert.assertFalse(
                testInstanceState
                        .setParams(false, true, true, false, true, true, true, true)
                        .isHealthy());
        Assert.assertFalse(
                testInstanceState
                        .setParams(false, true, true, true, true, true, false, true)
                        .isHealthy());
        Assert.assertFalse(
                testInstanceState
                        .setParams(false, true, true, true, false, false, true, true)
                        .isHealthy());
        Assert.assertFalse(
                testInstanceState
                        .setParams(false, true, true, true, false, false, true, false)
                        .isHealthy());
    }

    private class TestInstanceState {
        private final InstanceState instanceState;

        TestInstanceState(InstanceState instanceState1) {
            this.instanceState = instanceState1;
        }

        InstanceState setParams(
                boolean isRestoring,
                boolean isYmlWritten,
                boolean isCassandraProcessAlive,
                boolean isGossipEnabled,
                boolean isThriftEnabled,
                boolean isNativeEnabled,
                boolean isRequiredDirectoriesExist,
                boolean shouldCassandraBeAlive) {
            instanceState.setYmlWritten(isYmlWritten);
            instanceState.setCassandraProcessAlive(isCassandraProcessAlive);
            instanceState.setIsNativeTransportActive(isNativeEnabled);
            instanceState.setIsThriftActive(isThriftEnabled);
            instanceState.setIsGossipActive(isGossipEnabled);
            instanceState.setIsRequiredDirectoriesExist(isRequiredDirectoriesExist);
            instanceState.setShouldCassandraBeAlive(shouldCassandraBeAlive);

            if (isRestoring) instanceState.setRestoreStatus(Status.STARTED);
            else instanceState.setRestoreStatus(Status.FINISHED);

            return instanceState;
        }
    }
}
