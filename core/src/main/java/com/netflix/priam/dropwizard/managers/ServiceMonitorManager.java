package com.netflix.priam.dropwizard.managers;

import com.bazaarvoice.badger.api.BadgerRegistration;
import com.bazaarvoice.badger.api.BadgerRegistrationBuilder;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.MonitoringConfiguration;
import com.yammer.dropwizard.config.HttpConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@Singleton
public class ServiceMonitorManager implements Managed {

    private MonitoringConfiguration monitoringConfiguration;
    private ZooKeeperConnection zooKeeperConnection;
    private HttpConfiguration httpConfiguration;
    private BadgerRegistration badgerRegistration;

    @Inject
    public ServiceMonitorManager(MonitoringConfiguration monitoringConfiguration,
                                 Optional<ZooKeeperConnection> zooKeeperConnection,
                                 HttpConfiguration httpConfiguration) {
        this.monitoringConfiguration = monitoringConfiguration;
        this.zooKeeperConnection = zooKeeperConnection.orNull();
        this.httpConfiguration = httpConfiguration;
    }

    @Override
    public void start() throws Exception {
        if (zooKeeperConnection == null) {
            return;  // Disabled
        }

        if (monitoringConfiguration.getDefaultBadgerRegistrationState()) {
            register();
        } else {
            deregister();
        }
    }

    @Override
    public void stop() throws Exception {
        deregister();
    }

    public synchronized boolean isRegistered() {
        return badgerRegistration != null;
    }

    public synchronized void register() {
        if (zooKeeperConnection == null || badgerRegistration != null) {
            return;
        }

        // If ZooKeeper is configured, start Badger external monitoring
        String badgerServiceName = format(monitoringConfiguration.getBadgerServiceName());
        badgerRegistration = new BadgerRegistrationBuilder(zooKeeperConnection, badgerServiceName)
                .withVerificationPath(httpConfiguration.getPort(), "/v1/cassadmin/pingthrift")
                .withVersion(this.getClass().getPackage().getImplementationVersion())
                .withAwsTags()
                .register();
    }

    public synchronized void deregister() {
        if (badgerRegistration != null) {
            badgerRegistration.unregister(10, TimeUnit.SECONDS);
            badgerRegistration = null;
        }
    }
}
