package com.netflix.priam.cli;

import com.google.inject.AbstractModule;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.SDBInstanceFactory;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.defaultimpl.ClearCredential;
import com.netflix.priam.defaultimpl.PriamConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;
import com.netflix.priam.utils.TokenManager;

class LightGuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(IConfiguration.class).to(PriamConfiguration.class).asEagerSingleton();
        bind(ICredential.class).to(ClearCredential.class);
        bind(IPriamInstanceFactory.class).to(SDBInstanceFactory.class);
        bind(IMembership.class).to(StaticMembership.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(Sleeper.class).to(ThreadSleeper.class);
        bind(ITokenManager.class).to(TokenManager.class);
    }
}

