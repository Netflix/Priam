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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.netflix.priam.utils.SystemUtils;

public class InstanceDataRetrieverBase {
	protected JSONObject identityDocument = null;
	
	/*
	 * @return the id (e.g. 12345) of the AWS account of running instance, could be null /empty.
	 */
	public String getAWSAccountId() throws JSONException {
		if (this.identityDocument == null ) {
			String jsonStr = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/dynamic/instance-identity/document");
			synchronized(this) {
				if (this.identityDocument == null) {
					this.identityDocument = new JSONObject(jsonStr);
				}
			}
		}
		
		return this.identityDocument.getString("accountId");
	}

	/*
	 * @return the region (e.g. us-east-1) of the AWS account of running instance, could be null /empty.
	 */
	public String getRegion() throws JSONException {
		if (this.identityDocument == null ) {
			String jsonStr = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/dynamic/instance-identity/document");
			synchronized(this.identityDocument) {
				if (this.identityDocument == null) {
					this.identityDocument = new JSONObject(jsonStr);
				}
			}
		}
		
		return this.identityDocument.getString("region");
	}

	/*
	 * @return e.g.. us-east-1c
	 */
	public String getAvailabilityZone() throws JSONException {
		if (this.identityDocument == null ) {
			String jsonStr = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/dynamic/instance-identity/document");
			synchronized(this.identityDocument) {
				if (this.identityDocument == null) {
					this.identityDocument = new JSONObject(jsonStr);
				}
			}
		}
		
		return this.identityDocument.getString("availabilityZone");
	}
}
