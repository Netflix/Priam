package com.netflix.priam.backupv2;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.netflix.priam.backupv2.BackupPaths.isMetaFilePath;

public class DeletionQueueImpl implements DeletionQueue {

    private static final int MINIMUM_SSTABLE_COMPONENTS = 7;
    private static final int BATCH_SIZE = 1000;
    private final BlobStoreBucket bucket;
    private final Thrower thrower;
    private final Multimap<String, String> staging;
    private final List<String> filesToDelete;

    @Inject
    public DeletionQueueImpl(BlobStoreBucket bucket, ThrowerFactory throwerFactory) {
        this.bucket = bucket;
        this.thrower = throwerFactory.create(LoggerFactory.getLogger(DeletionQueueImpl.class));
        staging = HashMultimap.create();
        filesToDelete = new ArrayList<>(BATCH_SIZE);
    }

    @Override
    public void add(String path) throws BackupDeletionException {
        if (isMetaFilePath(path)) {
            filesToDelete.add(path);
            if (filesToDelete.size() >= BATCH_SIZE) {
                flush();
            }
        } else {
            String key = BackupPaths.getSSTablePrefix(path);
            staging.put(key, path);
            Collection<String> components = staging.get(key);
            if (isCompleteSSTable(components)) {
                if (components.size() + filesToDelete.size() >= BATCH_SIZE) {
                    flush();
                }
                filesToDelete.addAll(components);
                staging.removeAll(key);
            }
        }
    }

    private boolean isCompleteSSTable(@Nonnull Collection<String> components) {
        return components.size() >= MINIMUM_SSTABLE_COMPONENTS;
    }

    @Override
    public void flush() throws BackupDeletionException {
        try {
            bucket.delete(filesToDelete);
        } catch (Exception e) {
            String msg = String.format("keys attempted: %d, example: %s", filesToDelete.size(), filesToDelete.get(0));
            thrower.abort(Failure.FAILED_DELETING_FILES, msg, e);
        }
        filesToDelete.clear();
    }

    @Override
    public void reset() {
        staging.clear();
        filesToDelete.clear();
    }
}
