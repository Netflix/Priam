package com.netflix.priam.dropwizard;

import com.bazaarvoice.badger.api.BadgerRegistrationBuilder;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
    protected void initialize(PriamConfiguration configuration, Environment environment) throws Exception {

        Injector injector = Guice.createInjector(new PriamGuiceModule(configuration));
        try {
            configuration.getAmazonConfiguration().discoverConfiguration(injector.getInstance(ICredential.class));
            environment.manage(injector.getInstance(PriamServer.class));
            environment.manage(injector.getInstance(ZooKeeperRegistration.class));

            environment.addResource(injector.getInstance(BackupServlet.class));
            environment.addResource(injector.getInstance(CassandraAdmin.class));
            environment.addResource(injector.getInstance(CassandraConfig.class));
            environment.addResource(injector.getInstance(PriamInstanceResource.class));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }

        // Start Badger external monitoring
        String badgerServiceName = format("cassandra.%s", configuration.getCassandraConfiguration().getClusterName());
        new BadgerRegistrationBuilder(injector.getInstance(ZooKeeperConnection.class), badgerServiceName)
                .withVerificationPath(configuration.getHttpConfiguration().getPort(), "/v1/cassadmin/pingthrift")
                .register();
    }
}
