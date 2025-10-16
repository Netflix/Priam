package com.netflix.priam.backupv2;

import com.netflix.priam.aws.RemoteBackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;

public class SnapshotValidationMarkerWriter {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotValidationMarkerWriter.class);
    private static final String METADATA_PREFIX = "metadata";
    private final IConfiguration config;
    private final InstanceInfo instanceInfo;
    private final Provider<AbstractBackupPath> pathProvider;
    private final IBackupFileSystem fs;

    @Inject
    public SnapshotValidationMarkerWriter(
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
            fs.putObject(config.getBackupPrefix(), key, remotePath);
        } catch (Exception e) {
            logger.error("Failed to put snapshot verification marker. bucket: {}, key: {}, value: {}, message: {}",
                    config.getBackupPrefix(),
                    key,
                    remotePath,
                    e.getLocalizedMessage());
        }
    }
}
