package com.netflix.priam.aws;

import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.TestModule;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.config.InstanceInfo;
import org.junit.Before;
import org.junit.Test;

/* Tests of {@link UpdateSecuritySettings.java} */
public class TestUpdateSecuritySettings {
    private static final int PORT = 7103;

    private UpdateSecuritySettings updateSecuritySettings;
    private IMembership membership;
    private IPriamInstanceFactory factory;
    private FakeConfiguration config;
    private InstanceInfo instanceInfo;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new TestModule());
        factory = injector.getInstance(IPriamInstanceFactory.class);
        membership = injector.getInstance(IMembership.class);
        updateSecuritySettings = injector.getInstance(UpdateSecuritySettings.class);
        config = (FakeConfiguration) injector.getInstance(IConfiguration.class);
        instanceInfo = injector.getInstance(InstanceInfo.class);
    }

    @Test
    public void add_membershipEmpty() { // edge-case, not expected
        addToFactory(1, "1.1.1.1");
        Truth.assertThat(membership.listACL(PORT, PORT)).doesNotContain("1.1.1.1/32");
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains("1.1.1.1/32");
    }

    @Test
    public void add() {
        addToFactory(1, "1.1.1.1");
        membership.addACL(ImmutableSet.of("2.2.2.2/32"), PORT, PORT);
        Truth.assertThat(membership.listACL(PORT, PORT)).doesNotContain("1.1.1.1/32");
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains("1.1.1.1/32");
    }

    @Test
    public void delete() {
        addToFactory(1, "1.1.1.1");
        membership.addACL(ImmutableSet.of("2.2.2.2/32"), PORT, PORT);
        Truth.assertThat(membership.listACL(PORT, PORT)).contains("2.2.2.2/32");
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).doesNotContain("2.2.2.2/32");
    }

    @Test
    public void delete_factoryEmpty() { // edge-case, not expected
        membership.addACL(ImmutableSet.of("2.2.2.2/32"), PORT, PORT);
        Truth.assertThat(membership.listACL(PORT, PORT)).contains("2.2.2.2/32");
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).doesNotContain("2.2.2.2/32");
    }

    @Test
    public void addMyPrivateIP() {
        config.usePrivateIP(true);
        String ingressRule = instanceInfo.getPrivateIP() + "/32";
        Truth.assertThat(membership.listACL(PORT, PORT)).doesNotContain(ingressRule);
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains(ingressRule);
    }

    @Test
    public void addMyPublicIP() {
        config.usePrivateIP(false);
        String ingressRule = instanceInfo.getHostIP() + "/32";
        Truth.assertThat(membership.listACL(PORT, PORT)).doesNotContain(ingressRule);
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains(ingressRule);
    }

    @Test
    public void keepMyPrivateIP() {
        config.usePrivateIP(true);
        String ingressRule = instanceInfo.getPrivateIP() + "/32";
        membership.addACL(ImmutableSet.of(ingressRule), PORT, PORT);
        Truth.assertThat(membership.listACL(PORT, PORT)).contains(ingressRule);
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains(ingressRule);
    }

    @Test
    public void keepMyPublicIP() {
        config.usePrivateIP(false);
        String ingressRule = instanceInfo.getHostIP() + "/32";
        membership.addACL(ImmutableSet.of(ingressRule), PORT, PORT);
        Truth.assertThat(membership.listACL(PORT, PORT)).contains(ingressRule);
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains(ingressRule);
    }

    private void addToFactory(int id, String ip) {
        factory.create("myApp", id, "i-1", "hostname", ip, "us-east-1a", null /* volumes */, "123");
    }
}
