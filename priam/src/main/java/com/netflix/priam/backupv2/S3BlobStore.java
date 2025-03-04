package com.netflix.priam.backupv2;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.priam.aws.S3Iterator;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

public class S3BlobStore implements BlobStore {

    private final AmazonS3 s3Client;

    @Inject
    public S3BlobStore(@Named("awss3roleassumption") IS3Credential cred,
                       S3Context ) {
        this.s3Client =
                AmazonS3Client.builder()
                        .withCredentials(cred.getAwsCredentialProvider())
                        .withRegion(ion())
                        .build();
    }

    @Override
    public Future<Path> get(AbstractBackupPath path, int retry) throws BackupRestoreException, RejectedExecutionException {
    }

    @Override
    public Iterator<String> list(String prefix, String marker) {
        reutrn new S3Iterator(s3Client, getShard(), JOINER.join(globalPrefix, BackupTTLTask.FileType.SST_V2.toString()), null, null);
    }

    @Override
    public void delete(List<String> remotePaths) throws BackupRestoreException {
    }

    @Override
    public ListenableFuture<AbstractBackupPath> putAndDeleteLocalFile(Path path, Instant target, boolean async) throws FileNotFoundException, RejectedExecutionException, BackupRestoreException {
        return null;
    }

    @Override
    public long sizeOf(String remotePath) throws BackupRestoreException {
        return 0;
    }

    @Override
    public boolean head(String remotePath) {
        return false;
    }
}
