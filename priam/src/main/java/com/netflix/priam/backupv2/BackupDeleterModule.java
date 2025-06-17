package com.netflix.priam.backupv2;

import com.amazonaws.util.EC2MetadataUtils;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class BackupDeleterModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(BlobStoreBucket.class).to(S3BlobStoreBucket.class);
        bind(BackupDeleterConfiguration.class).to(BackupDeleterConfigurationImpl.class);
        bind(DeletionQueue.class).to(DeletionQueueImpl.class).asEagerSingleton();
    }

    @Provides
    S3Client S3ClientProvider() {
        String region = EC2MetadataUtils.getEC2InstanceRegion();
        return S3Client.builder()
                .credentialsProvider(InstanceProfileCredentialsProvider.create())
                .region(Region.of(region))
                .build();
    }

    @Named("s3bucket")
    @Provides
    String S3BucketNameProvider() {
        String region = EC2MetadataUtils.getEC2InstanceRegion();
        String env =  System.getenv("NETFLIX_ENVIRONMENT");
        return region.replaceAll("-", "") + "-cass-" + env + "-1";
    }
}
