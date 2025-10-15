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
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

@Singleton
public class S3RoleAssumptionCredential implements IS3Credential {
    private static final String AWS_ROLE_ASSUMPTION_SESSION_NAME = "S3RoleAssumptionSession";
    private static final Logger logger = LoggerFactory.getLogger(S3RoleAssumptionCredential.class);

    private final IS3Credential cred;
    private final IConfiguration config;
    private AwsCredentialsProvider stsSessionCredentialsProvider;

    @Inject
    public S3RoleAssumptionCredential(IS3Credential cred, IConfiguration config) {
        this.cred = cred;
        this.config = config;
    }

    @Override
    public AwsCredentials getCredentials() throws Exception {
        if (this.stsSessionCredentialsProvider == null) {
            this.getAwsCredentialProvider();
        }
        return this.stsSessionCredentialsProvider.resolveCredentials();
    }

    /*
     * Accessing an AWS resource requires a valid login token and credentials.  Both information is provided by the provider.
     * In addition, both login token and credentials can expire after a certain duration.  If expired,
     * the client needs to ask the provider to 'refresh" the information, hence the purpose of this behavior.
     *
     * TODO: this behavior needs to be part of the interface IS3Credential
     *
     */
    public void refresh() {
        // In SDK v2, credentials are refreshed automatically by the provider
        // We can force a refresh by getting new credentials
        if (this.stsSessionCredentialsProvider != null) {
            this.stsSessionCredentialsProvider.resolveCredentials();
        }
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialProvider() {
        if (this.stsSessionCredentialsProvider == null) {
            synchronized (this) {
                if (this.stsSessionCredentialsProvider == null) {
                    final String roleArn = this.config.getAWSRoleAssumptionArn();
                    // IAM role created for bucket own by account "awsprodbackup"
                    if (roleArn == null || roleArn.isEmpty()) {
                        logger.warn(
                                "Role ARN is null or empty probably due to missing config entry. Falling back to instance level credentials");
                        this.stsSessionCredentialsProvider = this.cred.getAwsCredentialProvider();
                    } else {
                        // Get handle to an implementation that uses AWS Security Token Service
                        // (STS) to create temporary, short-lived session with explicit refresh for
                        // session/token expiration.
                        try {
                            StsClient stsClient = StsClient.builder()
                                    .credentialsProvider(this.cred.getAwsCredentialProvider())
                                    .build();

                            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                                    .roleArn(roleArn)
                                    .roleSessionName(AWS_ROLE_ASSUMPTION_SESSION_NAME)
                                    .build();

                            this.stsSessionCredentialsProvider =
                                    StsAssumeRoleCredentialsProvider.builder()
                                            .stsClient(stsClient)
                                            .refreshRequest(assumeRoleRequest)
                                            .build();

                        } catch (Exception ex) {
                            throw new IllegalStateException(
                                    "Exception in getting handle to AWS Security Token Service (STS). Msg: "
                                            + ex.getLocalizedMessage(),
                                    ex);
                        }
                    }
                }
            }
        }
        return this.stsSessionCredentialsProvider;
    }
}
