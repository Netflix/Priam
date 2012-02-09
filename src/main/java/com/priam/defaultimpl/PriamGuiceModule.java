package com.priam.defaultimpl;

import com.amazonaws.auth.AWSCredentials;
import com.google.inject.AbstractModule;
import com.priam.aws.*;
import com.priam.backup.AbstractBackupPath;
import com.priam.backup.Consumer;
import com.priam.backup.IBackupFileSystem;
import com.priam.backup.IRestoreTokenSelector;
import com.priam.conf.IConfiguration;
import com.priam.identity.IMembership;
import com.priam.identity.IPriamInstanceFactory;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class PriamGuiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
        bind(IConfiguration.class).to(PriamConfiguration.class).asEagerSingleton();
        bind(IPriamInstanceFactory.class).to(SDBInstanceFactory.class).asEagerSingleton();
        bind(IMembership.class).to(AWSMembership.class);
        bind(AWSCredentials.class).to(ClearCredential.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class).asEagerSingleton();
        bind(Consumer.class).to(EBSConsumer.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(IRestoreTokenSelector.class).to(RestoreTokenSelector.class);
    }
}
