package com.netflix.priam.backupv2;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

public class S3BlobStoreBucket implements BlobStoreBucket {

    private final S3Client s3Client;
    private final String name;

    @Inject
    public S3BlobStoreBucket(Provider<S3Client> s3Client, @Named("s3bucket") String name) {
        this.s3Client = s3Client.get();
        this.name = name;
    }

    @Override
    public InputStream get(String key) {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(name)
                    .key(key)
                    .build();
            return s3Client.getObject(getObjectRequest);
    }

    @Override
    public Iterator<String> list(String prefix) {
        ListObjectsV2Request.Builder listReq = ListObjectsV2Request.builder().bucket(name).prefix(prefix);
        SdkIterable<S3Object> objects = s3Client.listObjectsV2Paginator(listReq.build()).contents();
        return new TransformIterator<>(objects.iterator(), S3Object::key);
    }

    @Override
    public Iterator<String> commonPrefixes(String prefix, String delimiter, String startAfter) {
        ListObjectsV2Request.Builder listReq =
                ListObjectsV2Request.builder()
                        .bucket(name)
                        .prefix(prefix)
                        .delimiter(delimiter)
                        .startAfter(startAfter);
        SdkIterable<CommonPrefix> commonPrefixes = s3Client.listObjectsV2Paginator(listReq.build()).commonPrefixes();
        return new TransformIterator<>(commonPrefixes.iterator(), CommonPrefix::prefix);
    }

    @Override
    public void delete(@Nonnull List<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        List<ObjectIdentifier> objectIds =
                keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .collect(Collectors.toList());
        Delete delete = Delete.builder().objects(objectIds).quiet(true).build();
        s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(name).delete(delete).build());
    }

    @Override
    public ListenableFuture<AbstractBackupPath> putAndDeleteLocalFile(Path path, Instant target, boolean async) throws FileNotFoundException, RejectedExecutionException, BackupRestoreException {
        throw new NotImplementedException();
    }

    @Override
    public long sizeOf(String remotePath) {
        throw new NotImplementedException();
    }

    @Override
    public boolean head(String remotePath) {
        throw new NotImplementedException();
    }

    @Override
    public String getName() {
        return name;
    }
}
