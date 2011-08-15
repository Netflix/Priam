package com.priam.defaultimpl;

import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
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

public class GuiceModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(IConfiguration.class).to(DefaultConfiguration.class);
        // bind(IPriamInstanceFactory.class).to(NFInstanceFactory.class);
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(PropertyCredential.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class);
        bind(Consumer.class).to(EBSConsumer.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
    }

}
