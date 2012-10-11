package com.netflix.priam.dropwizard;

import com.bazaarvoice.badger.api.BadgerRegistrationBuilder;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.netflix.priam.ICredential;
import com.netflix.priam.PriamServer;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.defaultimpl.PriamGuiceModule;
import com.netflix.priam.resources.BackupServlet;
import com.netflix.priam.resources.CassandraAdmin;
import com.netflix.priam.resources.CassandraConfig;
import com.netflix.priam.resources.PriamInstanceResource;
import com.netflix.priam.zookeeper.ZooKeeperRegistration;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class PriamService extends Service<PriamConfiguration> {
    protected static final Logger logger = LoggerFactory.getLogger(PriamService.class);

    public static void main(String[] args) throws Exception {
        new PriamService().run(args);
    }

    public PriamService() {
        super("priam");
    }

    @Override
    protected void initialize(PriamConfiguration config, Environment environment) throws Exception {

        Injector injector = Guice.createInjector(new PriamGuiceModule(config));
        try {
            config.getAmazonConfiguration().discoverConfiguration(injector.getInstance(ICredential.class));
            environment.manage(injector.getInstance(PriamServer.class));
            environment.manage(injector.getInstance(ZooKeeperRegistration.class));

            environment.addResource(injector.getInstance(BackupServlet.class));
            environment.addResource(injector.getInstance(CassandraAdmin.class));
            environment.addResource(injector.getInstance(CassandraConfig.class));
            environment.addResource(injector.getInstance(PriamInstanceResource.class));

            // If ZooKeeper is configured, start Badger external monitoring
            Optional<ZooKeeperConnection> zkConnection =
                    injector.getInstance(Key.get(new TypeLiteral<Optional<ZooKeeperConnection>>() {}));
            if (zkConnection.isPresent()) {
                String badgerServiceName = format("cassandra.%s", config.getCassandraConfiguration().getClusterName());
                new BadgerRegistrationBuilder(zkConnection.get(), badgerServiceName)
                        .withVerificationPath(config.getHttpConfiguration().getPort(), "/v1/cassadmin/pingthrift")
                        .register();
                environment.manage(new ManagedCloseable(zkConnection.get()));
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
