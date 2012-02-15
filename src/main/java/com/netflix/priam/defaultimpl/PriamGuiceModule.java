package com.netflix.priam.defaultimpl;

import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.inject.AbstractModule;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.AWSMembership;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.SDBInstanceFactory;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;

public class PriamGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
        bind(IConfiguration.class).to(PriamConfiguration.class).asEagerSingleton();
        bind(IPriamInstanceFactory.class).to(SDBInstanceFactory.class);
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(ClearCredential.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
    }
}
