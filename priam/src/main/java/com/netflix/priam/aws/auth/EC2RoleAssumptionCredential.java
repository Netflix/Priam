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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.identity.config.AWSInstanceInfo;
import com.netflix.priam.identity.config.InstanceInfo;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;

public class EC2RoleAssumptionCredential implements ICredential {
    private static final String AWS_ROLE_ASSUMPTION_SESSION_NAME = "AwsRoleAssumptionSession";
    private final ICredential cred;
    private final IConfiguration config;
    private final InstanceInfo instanceInfo;
    private AWSCredentialsProvider stsSessionCredentialsProvider;

    @Inject
    public EC2RoleAssumptionCredential(ICredential cred, IConfiguration config) {
        this.cred = cred;
        this.config = config;
        this.instanceInfo = new AWSInstanceInfo(cred);
    }

    @Override
    public AWSCredentialsProvider getAwsCredentialProvider() {
        if (this.config.isDualAccount() || this.stsSessionCredentialsProvider == null) {
            synchronized (this) {
                if (this.stsSessionCredentialsProvider == null) {

                    String roleArn;
                    /**
                     * Create the assumed IAM role based on the environment. For example, if the
                     * current environment is VPC, then the assumed role is for EC2 classic, and
                     * vice versa.
                     */
                    if (instanceInfo.getInstanceEnvironment()
                            == InstanceInfo.InstanceEnvironment.CLASSIC) {
                        roleArn = this.config.getClassicEC2RoleAssumptionArn();
                        // Env is EC2 classic --> IAM assumed role for VPC created
                    } else {
                        roleArn = this.config.getVpcEC2RoleAssumptionArn();
                        // Env is VPC --> IAM assumed role for EC2 classic created.
                    }

                    //
                    if (StringUtils.isEmpty(roleArn))
                        throw new NullPointerException(
                                "Role ARN is null or empty probably due to missing config entry");

                    /**
                     * Get handle to an implementation that uses AWS Security Token Service (STS) to
                     * create temporary, short-lived session with explicit refresh for session/token
                     * expiration.
                     */
                    try {
                        this.stsSessionCredentialsProvider =
                                new STSAssumeRoleSessionCredentialsProvider(
                                        this.cred.getAwsCredentialProvider(),
                                        roleArn,
                                        AWS_ROLE_ASSUMPTION_SESSION_NAME);

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
