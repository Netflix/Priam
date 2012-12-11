package com.netflix.priam.dropwizard.managers;

import com.bazaarvoice.badger.api.BadgerRegistrationBuilder;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.MonitoringConfiguration;
import com.netflix.priam.config.ZooKeeperConfiguration;
import com.yammer.dropwizard.config.HttpConfiguration;
import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.commons.io.IOUtils;

import static java.lang.String.format;

@Singleton
public class ServiceMonitorManager implements Managed {

    private MonitoringConfiguration monitoringConfiguration;
    private ZooKeeperConfiguration zooKeeperConfiguration;
    private HttpConfiguration httpConfiguration;
    private ZooKeeperConnection zooKeeperConnection;

    @Inject
    public ServiceMonitorManager(MonitoringConfiguration monitoringConfiguration, ZooKeeperConfiguration zooKeeperConfiguration, HttpConfiguration httpConfiguration) {
        this.monitoringConfiguration = monitoringConfiguration;
        this.zooKeeperConfiguration = zooKeeperConfiguration;
        this.httpConfiguration = httpConfiguration;
    }

    @Override
    public void start() throws Exception {
        if (zooKeeperConfiguration.isEnabled()) {
            return;
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
        return zooKeeperConnection != null;
    }

    public synchronized void register() {
        if (zooKeeperConnection != null) {
            return; // Already registered; nothing to do.
        }

        zooKeeperConnection = zooKeeperConfiguration.connect();

        // If ZooKeeper is configured, start Badger external monitoring
        String badgerServiceName = format(monitoringConfiguration.getBadgerServiceName());
        new BadgerRegistrationBuilder(zooKeeperConnection, badgerServiceName)
                .withVerificationPath(httpConfiguration.getPort(), "/v1/cassadmin/pingthrift")
                .withVersion(this.getClass().getPackage().getImplementationVersion())
                .register();
    }

    public synchronized void deregister() {
        if (zooKeeperConnection == null) {
            return; // Not registered; nothing to do.
        }
        IOUtils.closeQuietly(zooKeeperConnection);
        zooKeeperConnection = null;
    }

}
