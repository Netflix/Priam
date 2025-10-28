package com.netflix.priam.backupv2;

import com.amazonaws.services.s3.model.PutObjectResult;
import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;

public class SnapshotVerificationMarkerWriter {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotVerificationMarkerWriter.class);
    private static final String METADATA_PREFIX = "metadata";
    private final IConfiguration config;
    private final InstanceInfo instanceInfo;
    private final Provider<AbstractBackupPath> pathProvider;
    private final IBackupFileSystem fs;

    @Inject
    public SnapshotVerificationMarkerWriter(
            IConfiguration config,
            InstanceInfo instanceInfo,
            Provider<AbstractBackupPath> pathProvider,
            IBackupFileSystem fs) {
        this.config = config;
        this.instanceInfo = instanceInfo;
        this.pathProvider = pathProvider;
        this.fs = fs;
    }

    public void write(String remotePath) {
        AbstractBackupPath path = pathProvider.get();
        path.parseRemote(remotePath);
        String key = AbstractBackupPath.PATH_JOINER.join(
                METADATA_PREFIX,
                RemoteBackupPath.prependHash(path.getClusterName()),
                path.getLastModified().toEpochMilli(),
                instanceInfo.getRac(),
                path.getToken());
        try {
            new BoundedExponentialRetryCallable<Void>(0, 0, 3) {
                @Override
                public Void retriableCall() throws Exception {
                    fs.putObject(config.getBackupPrefix(), key, remotePath);
                    return null;
                }
            }.call();
        } catch (Exception e) {
            logger.error("Failed to put snapshot verification marker. bucket: {}, key: {}, value: {}, message: {}",
                    config.getBackupPrefix(),
                    key,
                    remotePath,
                    e.getLocalizedMessage());
        }
    }
}
