package com.netflix.priam.backupv2;

import com.google.inject.ImplementedBy;

import javax.annotation.Nonnull;
import java.nio.file.Path;

@ImplementedBy(BackupDeleterConfigurationImpl.class)
public interface BackupDeleterConfiguration {

    int getRetentionDays();

    int getMaxMetaFileReadRetries();

    @Nonnull
    Path getLocalMetaFileStagingDirectory();
}
