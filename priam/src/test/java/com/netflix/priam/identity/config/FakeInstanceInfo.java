/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.identity.config;

/** Created by aagrawal on 10/17/18. */
public class FakeInstanceInfo implements InstanceInfo {
    private String instanceId;
    private String availabilityZone;
    private String region;
    private String instanceType;
    private String asg;
    private String vpcId;

    public FakeInstanceInfo(String instanceId, String availabilityZone, String region) {
        this(instanceId, availabilityZone, region, "i2.xlarge", availabilityZone, "");
    }

    public FakeInstanceInfo(
            String instanceId,
            String availabilityZone,
            String region,
            String instanceType,
            String asg,
            String vpcId) {
        this.instanceId = instanceId;
        this.availabilityZone = availabilityZone;
        this.region = region;
        this.instanceType = instanceType;
        this.asg = asg;
        this.vpcId = vpcId;
    }

    @Override
    public String getRac() {
        return availabilityZone;
    }

    @Override
    public String getHostname() {
        return instanceId;
    }

    @Override
    public String getHostIP() {
        return "127.0.0.0";
    }

    @Override
    public String getPrivateIP() {
        return "127.1.1.0";
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public String getInstanceType() {
        return instanceType;
    }

    @Override
    public String getVpcId() {
        return vpcId;
    }

    @Override
    public String getRegion() {
        return region;
    }

    @Override
    public String getAutoScalingGroup() {
        return asg;
    }

    @Override
    public InstanceEnvironment getInstanceEnvironment() {
        return InstanceEnvironment.VPC;
    }

    public void setRac(String rac) {
        this.availabilityZone = rac;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
