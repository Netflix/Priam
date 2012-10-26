package com.netflix.priam.defaultimpl;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.AWSMembership;
import com.netflix.priam.aws.DefaultCredentials;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.aws.SDBInstanceRegistry;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.config.ZooKeeperConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TokenManagerProvider;
import com.netflix.priam.zookeeper.ZooKeeperRegistration;

public class PriamGuiceModule extends AbstractModule {
    private final PriamConfiguration priamConfiguration;

    public PriamGuiceModule(PriamConfiguration priamConfiguration) {
        this.priamConfiguration = priamConfiguration;
    }

    @Override
    protected void configure() {
        bind(CassandraConfiguration.class).toInstance(priamConfiguration.getCassandraConfiguration());
        bind(AmazonConfiguration.class).toInstance(priamConfiguration.getAmazonConfiguration());
        bind(BackupConfiguration.class).toInstance(priamConfiguration.getBackupConfiguration());
        bind(ZooKeeperConfiguration.class).toInstance(priamConfiguration.getZooKeeperConfiguration());
        bind(IPriamInstanceRegistry.class).to(SDBInstanceRegistry.class);
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(DefaultCredentials.class);
        bind(IBackupFileSystem.class).to(S3FileSystem.class);
        bind(AbstractBackupPath.class).to(S3BackupPath.class);
        bind(ICompression.class).to(SnappyCompression.class);
        bind(TokenManager.class).toProvider(TokenManagerProvider.class);
        bind(Sleeper.class).to(ThreadSleeper.class);
        bind(ZooKeeperRegistration.class).asEagerSingleton();
    }

    @Provides @Singleton
    Optional<ZooKeeperConnection> provideZooKeeperConnection() {
        ZooKeeperConfiguration zkConfiguration = priamConfiguration.getZooKeeperConfiguration();
        if (!zkConfiguration.isEnabled()) {
            return Optional.absent();
        }
        return Optional.of(zkConfiguration.connect());
    }
}
