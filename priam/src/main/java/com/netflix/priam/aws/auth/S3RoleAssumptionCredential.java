/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;

@Singleton
public class S3RoleAssumptionCredential implements IS3Credential {
	private static final String AWS_ROLE_ASSUMPTION_SESSION_NAME = "S3RoleAssumptionSession";
	private static final Logger logger = LoggerFactory.getLogger(S3RoleAssumptionCredential.class);

	private ICredential cred;
	private IConfiguration config;
	private AWSCredentialsProvider stsSessionCredentialsProvider;

	@Inject
	public S3RoleAssumptionCredential(ICredential cred, IConfiguration config) {
		this.cred = cred;
		this.config = config;		
	}
	
	@Override
	public AWSCredentials getCredentials() throws Exception {
				
		if (this.stsSessionCredentialsProvider == null) {
			this.getAwsCredentialProvider();
		}
		
		return this.stsSessionCredentialsProvider.getCredentials();
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
		this.cred.getAwsCredentialProvider().refresh();
	}

	@Override
	public AWSCredentialsProvider getAwsCredentialProvider() {
		if (this.stsSessionCredentialsProvider == null) {
			synchronized(this) {
				if (this.stsSessionCredentialsProvider == null) {
					
					final String roleArn = this.config.getAWSRoleAssumptionArn();  //IAM role created for bucket own by account "awsprodbackup"
					if (roleArn == null || roleArn.isEmpty()) {
						logger.warn("Role ARN is null or empty probably due to missing config entry. Falling back to instance level credentials");
						this.stsSessionCredentialsProvider =  this.cred.getAwsCredentialProvider();
						//throw new NullPointerException("Role ARN is null or empty probably due to missing config entry");
					}
					else {
						//== Get handle to an implementation that uses AWS Security Token Service (STS) to create temporary, short-lived session with explicit refresh for session/token expiration.
						try {

							this.stsSessionCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider(this.cred.getAwsCredentialProvider(), roleArn, AWS_ROLE_ASSUMPTION_SESSION_NAME);

						} catch (Exception ex) {
							throw new IllegalStateException("Exception in getting handle to AWS Security Token Service (STS).  Msg: " + ex.getLocalizedMessage(), ex);
						}
					}

				}
			}
		}
		
		return this.stsSessionCredentialsProvider;
	}
	
}