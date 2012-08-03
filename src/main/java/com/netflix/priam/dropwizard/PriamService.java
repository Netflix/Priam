package com.netflix.priam.dropwizard;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.defaultimpl.PriamConfiguration;
import com.netflix.priam.defaultimpl.PriamGuiceModule;
import com.netflix.priam.resources.BackupServlet;
import com.netflix.priam.resources.CassandraAdmin;
import com.netflix.priam.resources.CassandraConfig;
import com.netflix.priam.resources.PriamInstanceResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriamService extends Service<PriamConfiguration>
{
    protected static final Logger logger = LoggerFactory.getLogger(PriamService.class);

    public static void main(String[] args) throws Exception
    {
        new PriamService().run(args);
    }

    public PriamService()
    {
        super("priam");
    }

    @Override
    protected void initialize(PriamConfiguration priamConfiguration, Environment environment) throws Exception
    {
        Injector injector = Guice.createInjector(new PriamGuiceModule());
        try
        {
            injector.getInstance(IConfiguration.class).intialize();
            environment.manage(injector.getInstance(PriamServer.class));

            environment.addResource(injector.getInstance(BackupServlet.class));
            environment.addResource(injector.getInstance(CassandraAdmin.class));
            environment.addResource(injector.getInstance(CassandraConfig.class));
            environment.addResource(injector.getInstance(PriamInstanceResource.class));
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(),e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
