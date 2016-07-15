package com.netflix.priam.identity;

import com.google.inject.Singleton;
import com.netflix.priam.identity.config.InstanceDataRetriever;
import com.netflix.priam.identity.config.AWSVpcInstanceDataRetriever;

/*
 * A means to determine if running instance is within classic, default vpc account, or non-default vpc account
 */
@Singleton
public class AwsInstanceEnvIdentity implements InstanceEnvIdentity {

	private Boolean isClassic = false, isDefaultVpc = false, isNonDefaultVpc = false;
	
	public AwsInstanceEnvIdentity() {
		String vpcId = getVpcId();
		if (vpcId == null || vpcId.isEmpty()) {
			this.isClassic = true;
		} else {
			this.isNonDefaultVpc = true; //our instances run under a non default ("persistence_*") AWS acct
		}
	}
	
	/*
	 * @return the vpc id of the running instance, null if instance is not running within vpc.
	 */
	private String getVpcId() {
		InstanceDataRetriever insDataRetriever = new AWSVpcInstanceDataRetriever();
		return insDataRetriever.getVpcId();
	}

	@Override
	public Boolean isClassic() {
		return this.isClassic;
	}

	@Override
	public Boolean isDefaultVpc() {
		return this.isDefaultVpc;
	}

	@Override
	public Boolean isNonDefaultVpc() {
		return this.isNonDefaultVpc;
	}

}