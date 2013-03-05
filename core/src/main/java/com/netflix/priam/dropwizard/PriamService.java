package com.netflix.priam.dropwizard;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.ICredential;
import com.netflix.priam.PriamServer;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.defaultimpl.PriamGuiceModule;
import com.netflix.priam.dropwizard.managers.ManagedCloseable;
import com.netflix.priam.dropwizard.managers.ServiceMonitorManager;
import com.netflix.priam.dropwizard.managers.ServiceRegistryManager;
import com.netflix.priam.resources.BackupResource;
import com.netflix.priam.resources.CassandraAdminResource;
import com.netflix.priam.resources.CassandraConfigResource;
import com.netflix.priam.resources.MonitoringEnablementResource;
import com.netflix.priam.resources.PriamInstanceResource;
import com.netflix.priam.tools.CopyInstanceData;
import com.netflix.priam.tools.DeleteInstanceData;
import com.netflix.priam.tools.ListClusters;
import com.netflix.priam.tools.ListInstanceData;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriamService extends Service<PriamConfiguration> {
    protected static final Logger logger = LoggerFactory.getLogger(PriamService.class);

    public static void main(String[] args) throws Exception {
        new PriamService().run(args);
    }

    @Override
    public void initialize(Bootstrap<PriamConfiguration> bootstrap) {
        bootstrap.setName("priam");
        bootstrap.addCommand(new ListClusters());
        bootstrap.addCommand(new ListInstanceData());
        bootstrap.addCommand(new CopyInstanceData());
        bootstrap.addCommand(new DeleteInstanceData());
    }

    @Override
    public void run(PriamConfiguration config, Environment environment) throws Exception {
        // Protect from running multiple copies of Priam at the same time.  Jetty will enforce this because only one
        // instance can listen on 8080, but that check doesn't occur until the end of initialization which is too late.
        environment.manage(new ManagedCloseable(new JvmMutex(config.getJvmMutexPort())));

        // Don't ping www.terracotta.org on startup (Quartz).
        System.setProperty("org.terracotta.quartz.skipUpdateCheck", "true");

        Injector injector = Guice.createInjector(new PriamGuiceModule(config, environment));
        try {
            config.getAmazonConfiguration().discoverConfiguration(injector.getInstance(ICredential.class));

            environment.manage(injector.getInstance(PriamServer.class));
            environment.manage(injector.getInstance(ServiceRegistryManager.class));
            environment.manage(injector.getInstance(ServiceMonitorManager.class));

            environment.addResource(injector.getInstance(BackupResource.class));
            environment.addResource(injector.getInstance(CassandraAdminResource.class));
            environment.addResource(injector.getInstance(CassandraConfigResource.class));
            environment.addResource(injector.getInstance(PriamInstanceResource.class));
            environment.addResource(injector.getInstance(MonitoringEnablementResource.class));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
