package com.netflix.priam.health;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.BRTestModule;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test InstanceState
 * Created by aagrawal on 9/22/17.
 */
public class TestInstanceStatus {
    private TestInstanceState testInstanceState;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new BRTestModule());
        InstanceState instanceState = injector.getInstance(InstanceState.class);
        testInstanceState = new TestInstanceState(instanceState);
    }

    @Test
    public void testHealth(){
        //Verify good health.
        Assert.assertTrue(testInstanceState.setParams(true, true, true, true, true, true).isHealthy());
        Assert.assertTrue(testInstanceState.setParams(true, true, true, true, false, true).isHealthy());
        Assert.assertTrue(testInstanceState.setParams(true, true, true, false, true, true).isHealthy());

        //Negative health case scenarios.
        Assert.assertFalse(testInstanceState.setParams(false, true, true, false, true, true).isHealthy());
        Assert.assertFalse(testInstanceState.setParams(true, false, true, true, true, true).isHealthy());
        Assert.assertFalse(testInstanceState.setParams(true, true, false, true, true, true).isHealthy());
        Assert.assertFalse(testInstanceState.setParams(true, true, true, true, true, false).isHealthy());
        Assert.assertFalse(testInstanceState.setParams(true, true, true, false, false, true).isHealthy());
    }

    private class TestInstanceState{
        private InstanceState instanceState;

        TestInstanceState(InstanceState instanceState1){
            this.instanceState = instanceState1;
        }

        InstanceState setParams(boolean isYmlWritten, boolean isCassandraProcessAlive, boolean isGossipEnabled, boolean isThriftEnabled, boolean isNativeEnabled, boolean isRequiredDirectoriesExist){
            instanceState.setYmlWritten(isYmlWritten);
            instanceState.setCassandraProcessAlive(isCassandraProcessAlive);
            instanceState.setIsNativeTransportActive(isNativeEnabled);
            instanceState.setIsThriftActive(isThriftEnabled);
            instanceState.setIsGossipActive(isGossipEnabled);
            instanceState.setIsRequiredDirectoriesExist(isRequiredDirectoriesExist);
            return instanceState;
        }
    }
}
