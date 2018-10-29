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

import com.google.common.collect.ImmutableList;
import com.google.inject.ImplementedBy;
import com.netflix.priam.config.IConfiguration;
import java.util.List;

/** A means to fetch meta data of running instance */
@ImplementedBy(AWSInstanceInfo.class)
public interface InstanceInfo {
    /**
     * Get the availability zone of the running instance.
     *
     * @return the availability zone of the running instance. e.g. us-east-1c
     */
    String getRac();

    /**
     * Get the list of default racks available for this DC. This is used if no value is configured
     * for {@link IConfiguration#getRacs()}
     *
     * @return list of default racks.
     */
    default List<String> getDefaultRacks() {
        return ImmutableList.of(getRac());
    }

    /**
     * Get the hostname for the running instance. Cannot be null.
     *
     * @return the public hostname for the running instance. e.g.:
     *     ec2-12-34-56-78.compute-1.amazonaws.com, if available. Else return private ip address for
     *     running instance.
     */
    String getHostname();

    /**
     * Get ip address for running instance. Cannot be null.
     *
     * @return public ip if one is provided or private ip address for running instance.
     */
    String getHostIP();

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
     * Get the id of the vpc account for running instance.
     *
     * @return the id of the vpc account for running instance, null if does not exist.
     */
    String getVpcId(); // the id of the vpc for running instance

    /**
     * Get the region/data center of running instance
     *
     * @return the region of running instance, could be null/empty. (e.g. us-east-1)
     */
    String getRegion();

    /**
     * Get the ASG in which this instance is deployed. Note that Priam requires instances to be
     * under an ASG.
     *
     * @return the ASG of the instance. ex: cassandra_app--useast1e
     */
    String getAutoScalingGroup();

    /**
     * Environment of the current running instance. AWS only allows VPC environment (default).
     * Classic is deprecated environment by AWS.
     *
     * @return Environment of the current running instance.
     */
    InstanceEnvironment getInstanceEnvironment();

    enum InstanceEnvironment {
        CLASSIC,
        VPC
    }
}
