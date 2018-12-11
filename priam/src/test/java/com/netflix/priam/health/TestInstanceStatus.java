package com.netflix.priam.health;

import com.google.inject.Inject;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Test InstanceState Created by aagrawal on 9/22/17. */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
public class TestInstanceStatus {
    @Inject InstanceState instanceState;
    private TestInstanceState testInstanceState;

    @Before
    public void setUp() {
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
