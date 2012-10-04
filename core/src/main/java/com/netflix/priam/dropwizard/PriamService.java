package com.netflix.priam.dropwizard;

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

public class PriamService extends Service<PriamConfiguration> {
    protected static final Logger logger = LoggerFactory.getLogger(PriamService.class);

    public static void main(String[] args) throws Exception {
        new PriamService().run(args);
    }

    public PriamService() {
        super("priam");
    }

    @Override
    protected void initialize(PriamConfiguration priamConfiguration, Environment environment) throws Exception {

        Injector injector = Guice.createInjector(new PriamGuiceModule(priamConfiguration));
        try {
            priamConfiguration.getAmazonConfiguration().discoverConfiguration(injector.getInstance(ICredential.class));
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
    }
}
