package com.netflix.priam.backup;

import java.util.Arrays;
import java.util.Optional;

public enum BackupFolder {
    SNAPSHOTS("snapshots"),
    BACKUPS("backups");
    private String name;

    BackupFolder(String name) {
        this.name = name;
    }

    public static Optional<BackupFolder> fromName(String name) {
        return Arrays.stream(values()).filter(b -> b.name.equals(name)).findFirst();
    }
}
