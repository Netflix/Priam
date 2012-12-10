package com.netflix.priam.dropwizard;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.ICredential;
import com.netflix.priam.PriamServer;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.defaultimpl.PriamGuiceModule;
import com.netflix.priam.dropwizard.managers.ManagedCloseable;
import com.netflix.priam.dropwizard.managers.ServiceMonitorManager;
import com.netflix.priam.resources.BackupResource;
import com.netflix.priam.resources.CassandraAdminResource;
import com.netflix.priam.resources.CassandraConfigResource;
import com.netflix.priam.resources.PriamInstanceResource;
import com.netflix.priam.tools.CopyInstanceData;
import com.netflix.priam.tools.DeleteInstanceData;
import com.netflix.priam.tools.ListClusters;
import com.netflix.priam.tools.ListInstanceData;
import com.netflix.priam.dropwizard.managers.ServiceRegistryManager;
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
        addCommand(new ListClusters());
        addCommand(new ListInstanceData());
        addCommand(new CopyInstanceData());
        addCommand(new DeleteInstanceData());
    }

    @Override
    protected void initialize(PriamConfiguration config, Environment environment) throws Exception {
        // Protect from running multiple copies of Priam at the same time.  Jetty will enforce this because only one
        // instance can listen on 8080, but that check doesn't occur until the end of initialization which is too late.
        environment.manage(new ManagedCloseable(new JvmMutex(config.getJvmMutexPort())));

        // Don't ping www.terracotta.org on startup (Quartz).
        System.setProperty("org.terracotta.quartz.skipUpdateCheck", "true");

        Injector injector = Guice.createInjector(new PriamGuiceModule(config));
        try {
            config.getAmazonConfiguration().discoverConfiguration(injector.getInstance(ICredential.class));

            environment.manage(injector.getInstance(PriamServer.class));
            environment.manage(injector.getInstance(ServiceRegistryManager.class));
            environment.manage(injector.getInstance(ServiceMonitorManager.class));

            environment.addResource(injector.getInstance(BackupResource.class));
            environment.addResource(injector.getInstance(CassandraAdminResource.class));
            environment.addResource(injector.getInstance(CassandraConfigResource.class));
            environment.addResource(injector.getInstance(PriamInstanceResource.class));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
