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
import org.junit.Before;
import org.junit.Test;

/* Tests of {@link UpdateSecuritySettings.java} */
public class TestUpdateSecuritySettings {
    private static final int PORT = 7103;

    private UpdateSecuritySettings updateSecuritySettings;
    private IMembership membership;
    private IPriamInstanceFactory factory;
    private FakeConfiguration config;

    @Before
    public void setUp() {
        Injector injector = Guice.createInjector(new TestModule());
        factory = injector.getInstance(IPriamInstanceFactory.class);
        membership = injector.getInstance(IMembership.class);
        updateSecuritySettings = injector.getInstance(UpdateSecuritySettings.class);
        config = (FakeConfiguration) injector.getInstance(IConfiguration.class);
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
    public void dontDeleteOthersIngress() {
        config.setSkipDeletingOthersIngressRules(true);
        membership.addACL(ImmutableSet.of("1.1.1.1/32"), PORT, PORT);
        Truth.assertThat(membership.listACL(PORT, PORT)).contains("1.1.1.1/32");
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).contains("1.1.1.1/32");
    }

    @Test
    public void dontUpdateOthersIngress() {
        addToFactory(1, "1.1.1.1");
        config.setSkipUpdatingOthersIngressRules(true);
        Truth.assertThat(membership.listACL(PORT, PORT)).isEmpty();
        updateSecuritySettings.execute();
        Truth.assertThat(membership.listACL(PORT, PORT)).isEmpty();
    }

    private void addToFactory(int id, String ip) {
        factory.create("myApp", id, "i-1", "hostname", ip, "us-east-1a", null /* volumes */, "123");
    }
}
