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
