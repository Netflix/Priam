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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calls AWS metadata to get info on the location of the running instance within vpc environment.
 *
 */
public class AWSVpcInstanceDataRetriever extends InstanceDataRetrieverBase{
    private static final Logger logger = LoggerFactory.getLogger(AWSVpcInstanceDataRetriever.class);
    @Override
    public String getVpcId() {
        String nacId = getMac();
        if (nacId == null || nacId.isEmpty())
            return null;

        String vpcId = null;
        try {
            vpcId = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/network/interfaces/macs/" + nacId + "vpc-id").trim();
        } catch (Exception e) {
            logger.info("Vpc id does not exist for running instance, not fatal as running instance maybe not be in vpc.  Msg: {}", e.getLocalizedMessage());
        }

        return vpcId;
    }

    /** @return either the public hostname if we have one, otherwise the vpc local hostname */
    @Override
    public String getPublicHostname() {
        try {
            return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname");
        } catch (RuntimeException error) {
            // If we can't retrieve a public hostname, retrieve a VPC local one.
            return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-hostname");
        }
    }

    /** @return either the public ip if we have one, otherwise the vpc local ip */
    @Override
    public String getPublicIP() {
        try {
            return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4");
        } catch (RuntimeException error) {
            // If we can't retrieve a public hostname, retrieve a VPC local one.
            return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-ipv4");
        }
    }
}