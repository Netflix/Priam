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
package com.netflix.priam.identity.config;

import org.codehaus.jettison.json.JSONException;

/** A means to fetch meta data of running instance */
public interface InstanceDataRetriever {
    /**
     * Get the availability zone of the running instance.
     *
     * @return the availability zone of the running instance. e.g. us-east-1c
     */
    String getRac();

    /**
     * Get the public hostname for the running instance.
     *
     * @return the public hostname for the running instance. e.g.:
     *     ec2-12-34-56-78.compute-1.amazonaws.com
     */
    String getPublicHostname();

    /**
     * Get public ip address for running instance. Can be null.
     *
     * @return private ip address for running instance.
     */
    String getPublicIP();

    /**
     * Get private ip address for running instance.
     *
     * @return private ip address for running instance.
     */
    String getPrivateIP();

    /**
     * Get the instance id of the running instance.
     *
     * @return the instance id of the running instance. e.g. i-07a88a49ff155353
     */
    String getInstanceId();

    /**
     * Get the instance type of the running instance.
     *
     * @return the instance type of the running instance. e.g. i3.2xlarge
     */
    String getInstanceType();

    /**
     * Get the id of the network interface for running instance.
     *
     * @return id of the network interface for running instance
     */
    String getMac();

    /**
     * Get the id of the vpc account for running instance.
     *
     * @return the id of the vpc account for running instance, null if does not exist.
     */
    String getVpcId(); // the id of the vpc for running instance

    /**
     * AWS Account ID of running instance.
     *
     * @return the id (e.g. 12345) of the AWS account of running instance, could be null /empty.
     * @throws JSONException
     */
    String getAWSAccountId() throws JSONException;

    /**
     * Get the region of the AWS account of running instance
     *
     * @return the region (e.g. us-east-1) of the AWS account of running instance, could be null
     *     /empty.
     * @throws JSONException
     */
    String getRegion() throws JSONException;

    /**
     * Get the availability zone of the running instance.
     *
     * @return the availability zone of the running instance. e.g. us-east-1c
     * @throws JSONException
     */
    String getAvailabilityZone() throws JSONException;
}
