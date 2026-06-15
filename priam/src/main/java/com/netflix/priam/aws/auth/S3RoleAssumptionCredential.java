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
package com.netflix.priam.aws.auth;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class S3RoleAssumptionCredential implements ICredential {
    private static final String SESSION_NAME = "S3RoleAssumptionSession";
    private static final Logger logger = LoggerFactory.getLogger(S3RoleAssumptionCredential.class);

    private final ICredential cred;
    private final IConfiguration config;
    private AwsCredentialsProvider credentialsProvider;

    @Inject
    public S3RoleAssumptionCredential(@Named("s3") ICredential cred, IConfiguration config) {
        this.cred = cred;
        this.config = config;
    }

    public AwsCredentialsProvider getAwsCredentialProvider() {
        if (this.credentialsProvider == null) {
            synchronized (this) {
                if (this.credentialsProvider == null) {
                    final String roleArn = config.getAWSRoleAssumptionArn();
                    if (roleArn == null || roleArn.isEmpty()) {
                        logger.warn("Role ARN is empty due to missing config. Using instance level credentials");
                        credentialsProvider = cred.getAwsCredentialProvider();
                    } else {
                        try {
                            StsClient stsClient =
                                    StsClient.builder().credentialsProvider(cred.getAwsCredentialProvider()).build();
                            AssumeRoleRequest assumeRoleRequest =
                                    AssumeRoleRequest.builder().roleArn(roleArn).roleSessionName(SESSION_NAME).build();
                            credentialsProvider =
                                    StsAssumeRoleCredentialsProvider.builder()
                                            .stsClient(stsClient)
                                            .refreshRequest(assumeRoleRequest)
                                            .build();
                        } catch (Exception ex) {
                            throw new IllegalStateException(
                                    "Exception in getting handle to AWS Security Token Service (STS).  Msg: "
                                            + ex.getLocalizedMessage(),
                                    ex);
                        }
                    }
                }
            }
        }
        return credentialsProvider;
    }
}
