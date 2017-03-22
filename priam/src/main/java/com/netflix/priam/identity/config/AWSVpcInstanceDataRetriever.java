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
package com.netflix.priam.identity.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.identity.InstanceEnvIdentity;
import com.netflix.priam.utils.SystemUtils;

/**
* Calls AWS metadata to get info on the location of the running instance within vpc environment.
*
*/
public class AWSVpcInstanceDataRetriever  extends InstanceDataRetrieverBase implements InstanceDataRetriever {
	private static final Logger logger = LoggerFactory.getLogger(AWSVpcInstanceDataRetriever.class);

   public String getRac()
   {
       return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
   }

   public String getPublicHostname()
   {
       return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname");
   }

   public String getPublicIP()
   {
       return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4");
   }

   public String getInstanceId()
   {
       return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-id");
   }

   public String getInstanceType()
   {
       return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-type");
   }

	@Override
	/*
	 * @return id of the network interface for running instance
	 */
	public String getMac() {
		return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/network/interfaces/macs/").trim();
	}

	@Override
	/*
	 * @return the id of the vpc account for running instance, null if does not exist.
	 */
	public String getVpcId() {
		String nacId = getMac();
		if (nacId == null || nacId.isEmpty()) 
			return null;
		
		String vpcId = null;
		try {
			vpcId = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/network/interfaces/macs/" + nacId + "vpc-id").trim();			
		} catch (Exception e) {
			logger.info("Vpc id does not exist for running instance, not fatal as running instance maybe not be in vpc.  Msg: " + e.getLocalizedMessage());
		}

		return  vpcId;
	}

}