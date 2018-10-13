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
package com.netflix.priam.identity.token;

import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.Sleeper;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreGeneratedTokenRetriever extends TokenRetrieverBase
        implements IPreGeneratedTokenRetriever {

    private static final Logger logger = LoggerFactory.getLogger(PreGeneratedTokenRetriever.class);
    private final IPriamInstanceFactory factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private ListMultimap<String, PriamInstance> locMap;
    private InstanceInfo instanceInfo;

    @Inject
    public PreGeneratedTokenRetriever(
            IPriamInstanceFactory factory,
            IMembership membership,
            IConfiguration config,
            Sleeper sleeper,
            InstanceInfo instanceInfo) {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.sleeper = sleeper;
        this.instanceInfo = instanceInfo;
    }

    @Override
    public PriamInstance get() throws Exception {
        logger.info("Looking for any pre-generated token");

        final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
        List<String> asgInstances = membership.getRacMembership();
        // Sleep random interval - upto 15 sec
        sleeper.sleep(new Random().nextInt(5000) + 10000);
        for (PriamInstance dead : allIds) {
            // test same zone and is it is alive.
            if (!dead.getRac().equals(instanceInfo.getRac())
                    || asgInstances.contains(dead.getInstanceId())
                    || !isInstanceDummy(dead)) continue;
            logger.info("Found pre-generated token: {}", dead.getToken());
            PriamInstance markAsDead =
                    factory.create(
                            dead.getApp() + "-dead",
                            dead.getId(),
                            dead.getInstanceId(),
                            dead.getHostName(),
                            dead.getHostIP(),
                            dead.getRac(),
                            dead.getVolumes(),
                            dead.getToken());
            // remove it as we marked it down...
            factory.delete(dead);

            String payLoad = markAsDead.getToken();
            logger.info(
                    "Trying to grab slot {} with availability zone {}",
                    markAsDead.getId(),
                    markAsDead.getRac());
            return factory.create(
                    config.getAppName(),
                    markAsDead.getId(),
                    instanceInfo.getInstanceId(),
                    instanceInfo.getHostname(),
                    instanceInfo.getHostIP(),
                    instanceInfo.getRac(),
                    markAsDead.getVolumes(),
                    payLoad);
        }
        return null;
    }

    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        this.locMap = locMap;
    }
}
