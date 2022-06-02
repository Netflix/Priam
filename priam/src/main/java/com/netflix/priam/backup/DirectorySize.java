package com.netflix.priam.backup;

import com.google.inject.ImplementedBy;

/** estimates the number of bytes remaining to upload in a snapshot */
@ImplementedBy(SnapshotDirectorySize.class)
public interface DirectorySize {
    /** return the total bytes of all snapshot files south of location in the filesystem */
    long getBytes(String location);
}
