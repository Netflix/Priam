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

/*
 * A means to fetch meta data of running instance
 */
public interface InstanceDataRetriever {
    String getRac();
    String getPublicHostname();
    String getPublicIP();
    String getInstanceId();
    String getInstanceType();
    String getMac(); //fetch id of the network interface for running instance
    String getVpcId(); //the id of the vpc for running instance
    String getAWSAccountId()  throws JSONException; 
    String getRegion()  throws JSONException;
    String getAvailabilityZone()  throws JSONException;
}
