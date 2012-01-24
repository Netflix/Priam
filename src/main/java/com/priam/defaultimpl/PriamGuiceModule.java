package com.priam.defaultimpl;

import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.priam.aws.AWSMembership;
import com.priam.aws.EBSConsumer;
import com.priam.aws.ICredential;
import com.priam.aws.RestoreTokenSelector;
import com.priam.aws.S3BackupPath;
import com.priam.aws.S3FileSystem;
import com.priam.aws.SDBInstanceFactory;
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.Consumer;
import com.priam.backup.IBackupFileSystem;
import com.priam.backup.IRestoreTokenSelector;
import com.priam.conf.IConfiguration;
import com.priam.identity.IMembership;
import com.priam.identity.IPriamInstanceFactory;

public class PriamGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
        bind(IConfiguration.class).to(PriamConfiguration.class).asEagerSingleton();
        bind(IPriamInstanceFactory.class).to(SDBInstanceFactory.class).asEagerSingleton();
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(ClearCredential.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class).asEagerSingleton();
        bind(Consumer.class).to(EBSConsumer.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(IRestoreTokenSelector.class).to(RestoreTokenSelector.class);
    }
}
