package com.netflix.priam.aws.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.inject.Inject;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.identity.InstanceEnvIdentity;

public class EC2RoleAssumptionCredential implements ICredential {
	private static final String AWS_ROLE_ASSUMPTION_SESSION_NAME = "AwsRoleAssumptionSession";
	private ICredential cred;
	private IConfiguration config;
	private InstanceEnvIdentity insEnvIdentity;    
	private AWSCredentialsProvider stsSessionCredentialsProvider;

	@Inject
	public EC2RoleAssumptionCredential(ICredential cred, IConfiguration config, InstanceEnvIdentity insEnvIdentity) {
		this.cred = cred;
		this.config = config;
        this.insEnvIdentity = insEnvIdentity;
	}
	
	@Override
	public AWSCredentialsProvider getAwsCredentialProvider() {
		if (this.config.isDualAccount() || this.stsSessionCredentialsProvider == null) {
			synchronized(this) {
				if (this.stsSessionCredentialsProvider == null) {
					
					String roleArn = null;
					/**
					 *  Create the assumed IAM role based on the environment.
					 *  For example, if the current environment is VPC, 
					 *  then the assumed role is for EC2 classic, and vice versa.
					 */
					if (this.insEnvIdentity.isClassic()) {
						roleArn = this.config.getClassicEC2RoleAssumptionArn();      // Env is EC2 classic --> IAM assumed role for VPC created 
				    }
					else {
						roleArn = this.config.getVpcEC2RoleAssumptionArn();  // Env is VPC --> IAM assumed role for EC2 classic created 
					}
				
					//
					if (roleArn == null || roleArn.isEmpty()) 
						throw new NullPointerException("Role ARN is null or empty probably due to missing config entry");
					
					
					/**
					 *  Get handle to an implementation that uses AWS Security Token Service (STS) to create temporary, 
					 *  short-lived session with explicit refresh for session/token expiration.
					 */
					try {						
						this.stsSessionCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider(this.cred.getAwsCredentialProvider(), roleArn, AWS_ROLE_ASSUMPTION_SESSION_NAME);
						
					} catch (Exception ex) {
						throw new IllegalStateException("Exception in getting handle to AWS Security Token Service (STS).  Msg: " + ex.getLocalizedMessage(), ex);
					}							

				}

		   }
	   }
	   
	   return this.stsSessionCredentialsProvider;

	}
}