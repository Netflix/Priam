package com.priam.netflix;

import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.priam.aws.AWSMembership;
import com.priam.aws.EBSConsumer;
import com.priam.aws.ICredential;
import com.priam.aws.S3BackupPath;
import com.priam.aws.S3FileSystem;
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.Consumer;
import com.priam.backup.IBackupFileSystem;
import com.priam.conf.IConfiguration;
import com.priam.identity.IMembership;
import com.priam.identity.IPriamInstanceFactory;

public class GuiceModule extends AbstractModule
{

    public static enum BootSource
    {
        SIMPLEDB, CASSANDRA, LOCAL
    };

    private BootSource bootsource = BootSource.CASSANDRA;

    public GuiceModule(BootSource bootsource)
    {
        this.bootsource = bootsource;
    }

    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
        bind(IConfiguration.class).to(NFConfiguration.class).asEagerSingleton();
        switch (bootsource)
        {
        case LOCAL:
            bind(IPriamInstanceFactory.class).to(NFBootInstanceFactory.class).asEagerSingleton();
            break;
        case SIMPLEDB:
            bind(IPriamInstanceFactory.class).to(NFInstanceFactory.class).asEagerSingleton();
            break;
        case CASSANDRA:
        default:
            bind(IPriamInstanceFactory.class).to(CassandraInstanceFactory.class).asEagerSingleton();
            break;
        }
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(NFCredential.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class).asEagerSingleton();
        bind(Consumer.class).to(EBSConsumer.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
    }

}
