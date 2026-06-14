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
import com.netflix.priam.identity.config.InstanceInfo;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.inject.Inject;

public class EC2RoleAssumptionCredential implements ICredential {
    private static final String AWS_ROLE_ASSUMPTION_SESSION_NAME = "AwsRoleAssumptionSession";
    private final ICredential cred;
    private final IConfiguration config;
    private final InstanceInfo instanceInfo;
    private AwsCredentialsProvider stsSessionCredentialsProvider;

    @Inject
    public EC2RoleAssumptionCredential(
            ICredential cred, IConfiguration config, InstanceInfo instanceInfo) {
        this.cred = cred;
        this.config = config;
        this.instanceInfo = instanceInfo;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialProvider() {
        if (this.stsSessionCredentialsProvider == null) {
            synchronized (this) {
                if (this.stsSessionCredentialsProvider == null) {
                    String roleArn =
                            instanceInfo.getInstanceEnvironment() == InstanceInfo.InstanceEnvironment.CLASSIC
                                    ? this.config.getClassicEC2RoleAssumptionArn()
                                    : this.config.getVpcEC2RoleAssumptionArn();
                    Validate.notEmpty(roleArn, "roleArn is empty");
                    try {
                        StsClient stsClient =
                                StsClient.builder()
                                        .credentialsProvider(this.cred.getAwsCredentialProvider())
                                        .build();

                        AssumeRoleRequest assumeRoleRequest =
                                AssumeRoleRequest.builder()
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
                                "Exception in getting handle to AWS Security Token Service (STS).  Msg: "
                                        + ex.getLocalizedMessage(),
                                ex);
                    }
                }
            }
        }
        return this.stsSessionCredentialsProvider;
    }
}
