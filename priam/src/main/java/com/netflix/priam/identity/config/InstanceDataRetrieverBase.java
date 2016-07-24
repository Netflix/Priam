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