/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.priam.aws.auth.IS3Credential;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A version of S3FileSystem which allows it api access across different AWS accounts.
 *
 * *Note: ideally, this object should extend S3FileSystem but could not be done because:
 * - S3FileSystem is a singleton and it uses DI.  To follow the DI pattern, the best way to get this singleton is via injection.
 * - S3FileSystem registers a MBean to JMX which must be only once per JVM.  If not, you get
 * java.lang.RuntimeException: javax.management.InstanceAlreadyExistsException: com.priam.aws.S3FileSystemMBean:name=S3FileSystemMBean
 * -
 */
@Singleton
public class S3CrossAccountFileSystem {
    private static final Logger logger = LoggerFactory.getLogger(S3CrossAccountFileSystem.class);

    private AmazonS3 s3Client;
    private final S3FileSystem s3fs;
    private final IConfiguration config;
    private final IS3Credential s3Credential;
    private final InstanceInfo instanceInfo;

    @Inject
    public S3CrossAccountFileSystem(
            @Named("backup") IBackupFileSystem fs,
            @Named("awss3roleassumption") IS3Credential s3Credential,
            IConfiguration config,
            InstanceInfo instanceInfo) {

        this.s3fs = (S3FileSystem) fs;
        this.config = config;
        this.s3Credential = s3Credential;
        this.instanceInfo = instanceInfo;
    }

    public IBackupFileSystem getBackupFileSystem() {
        return this.s3fs;
    }

    public AmazonS3 getCrossAcctS3Client() {
        if (this.s3Client == null) {

            synchronized (this) {
                if (this.s3Client == null) {

                    try {

                        this.s3Client =
                                AmazonS3Client.builder()
                                        .withCredentials(s3Credential.getAwsCredentialProvider())
                                        .withRegion(instanceInfo.getRegion())
                                        .build();

                    } catch (Exception e) {
                        throw new IllegalStateException(
                                "Exception in getting handle to s3 client.  Msg: "
                                        + e.getLocalizedMessage(),
                                e);
                    }

                    // Lets leverage the IBackupFileSystem behaviors except we want it to use our
                    // amazon S3 client which has cross AWS account api capability.
                    this.s3fs.setS3Client(s3Client);
                }
            }
        }

        return this.s3Client;
    }
}
