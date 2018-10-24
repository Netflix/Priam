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
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewTokenRetriever extends TokenRetrieverBase implements INewTokenRetriever {

    private static final Logger logger = LoggerFactory.getLogger(NewTokenRetriever.class);
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private final ITokenManager tokenManager;
    private ListMultimap<String, PriamInstance> locMap;
    private InstanceInfo instanceInfo;

    @Inject
    // Note: do not parameterized the generic type variable to an implementation as it confuses
    // Guice in the binding.
    public NewTokenRetriever(
            IPriamInstanceFactory factory,
            IMembership membership,
            IConfiguration config,
            Sleeper sleeper,
            ITokenManager tokenManager,
            InstanceInfo instanceInfo) {
        this.factory = factory;
        this.membership = membership;
        this.config = config;
        this.sleeper = sleeper;
        this.tokenManager = tokenManager;
        this.instanceInfo = instanceInfo;
    }

    @Override
    public PriamInstance get() throws Exception {

        logger.info("Generating my own and grabbing new token");
        // Sleep random interval - upto 15 sec
        sleeper.sleep(new Random().nextInt(15000));
        int hash = tokenManager.regionOffset(instanceInfo.getRegion());
        // use this hash so that the nodes are spred far away from the other
        // regions.

        int max = hash;
        List<PriamInstance> allInstances = factory.getAllIds(config.getAppName());
        for (PriamInstance data : allInstances)
            max =
                    (data.getRac().equals(instanceInfo.getRac()) && (data.getId() > max))
                            ? data.getId()
                            : max;
        int maxSlot = max - hash;
        int my_slot;

        if (hash == max && locMap.get(instanceInfo.getRac()).size() == 0) {
            int idx = config.getRacs().indexOf(instanceInfo.getRac());
            if (idx < 0)
                throw new Exception(
                        String.format(
                                "Rac %s is not in Racs %s",
                                instanceInfo.getRac(), config.getRacs()));
            my_slot = idx + maxSlot;
        } else my_slot = config.getRacs().size() + maxSlot;

        logger.info(
                "Trying to createToken with slot {} with rac count {} with rac membership size {} with dc {}",
                my_slot,
                membership.getRacCount(),
                membership.getRacMembershipSize(),
                instanceInfo.getRegion());
        String payload =
                tokenManager.createToken(
                        my_slot,
                        membership.getRacCount(),
                        membership.getRacMembershipSize(),
                        instanceInfo.getRegion());
        return factory.create(
                config.getAppName(),
                my_slot + hash,
                instanceInfo.getInstanceId(),
                instanceInfo.getHostname(),
                instanceInfo.getHostIP(),
                instanceInfo.getRac(),
                null,
                payload);
    }

    /*
     * @param A map of the rac for each instance.
     */
    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        this.locMap = locMap;
    }
}
