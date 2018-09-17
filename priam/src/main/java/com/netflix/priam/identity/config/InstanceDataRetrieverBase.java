/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.identity.config;

import com.netflix.priam.utils.SystemUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public abstract class InstanceDataRetrieverBase implements InstanceDataRetriever{
    protected JSONObject identityDocument = null;

    public String getPrivateIP(){
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-ipv4");
    }

    public String getRac() {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    }

    public String getPublicHostname() {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname");
    }

    public String getPublicIP() {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4");
    }

    public String getInstanceId() {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-id");
    }

    public String getInstanceType() {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-type");
    }

    public String getMac() {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/network/interfaces/macs/").trim();
    }

    public String getAWSAccountId() throws JSONException {
        if (this.identityDocument == null) {
            String jsonStr = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/dynamic/instance-identity/document");
            this.identityDocument = new JSONObject(jsonStr);
        }
        return this.identityDocument.getString("accountId");
    }

    public String getRegion() throws JSONException {
        if (this.identityDocument == null) {
            String jsonStr = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/dynamic/instance-identity/document");
            this.identityDocument = new JSONObject(jsonStr);
        }
        return this.identityDocument.getString("region");
    }

    public String getAvailabilityZone() throws JSONException {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    }
}