package com.netflix.priam.backupv2;

import com.amazonaws.util.EC2MetadataUtils;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BackupDeleterConfigurationImpl implements BackupDeleterConfiguration {
    @Override
    public int getRetentionDays() {
        String env = System.getenv("NETFLIX_ENVIRONMENT");
        switch (env) {
            case "prod":
                String region = EC2MetadataUtils.getEC2InstanceRegion();
                return region.equals("us-east-1") ? 30 : 7;
            case "test":
                return 2;
            default:
                String message = "Saw invalid environment " + env + " while deriving retention.";
                throw new IllegalStateException(message);
        }
    }

    @Override
    public int getMaxMetaFileReadRetries() {
        return 3;
    }

    @Nonnull
    @Override
    public Path getLocalMetaFileStagingDirectory() {
        return Paths.get("/tmp/");
    }
}
