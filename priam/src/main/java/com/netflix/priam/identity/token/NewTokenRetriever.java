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
package com.netflix.priam.identity.token;

import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class NewTokenRetriever extends TokenRetrieverBase implements INewTokenRetriever {

    private static final Logger logger = LoggerFactory.getLogger(NewTokenRetriever.class);
    private IPriamInstanceFactory<PriamInstance> factory;
    private IMembership membership;
    private IConfiguration config;
    private Sleeper sleeper;
    private ITokenManager tokenManager;
    private ListMultimap<String, PriamInstance> locMap;

    @Inject
    //Note: do not parameterized the generic type variable to an implementation as it confuses Guice in the binding.
	public NewTokenRetriever(IPriamInstanceFactory factory, IMembership membership, IConfiguration config, Sleeper sleeper, ITokenManager tokenManager) {
		this.factory = factory;
		this.membership = membership;
		this.config = config;
		this.sleeper = sleeper;
		this.tokenManager = tokenManager;
	}
	
	@Override
	public PriamInstance get() throws Exception {
    	
		logger.info("Generating my own and new identifier");
        // Sleep random interval - upto 15 sec
        sleeper.sleep(new Random().nextInt(15000));
        int hash = tokenManager.regionOffset(config.getDC());
        // use this hash so that the nodes are spred far away from the other
        // regions.

        int max = hash;
        List<PriamInstance> allInstances = factory.getAllIds(config.getAppName());
        for (PriamInstance data : allInstances)
            max = (data.getRac().equals(config.getRac()) && (data.getId() > max)) ? data.getId() : max;
        int maxSlot = max - hash;
        int my_slot = 0;

        if (hash == max && locMap.get(config.getRac()).size() == 0) {
            int idx = config.getRacs().indexOf(config.getRac());
            if (idx < 0)
                throw new Exception(String.format("Rac %s is not in Racs %s", config.getRac(), config.getRacs()));
            my_slot = idx + maxSlot;
        } else
            my_slot = config.getRacs().size() + maxSlot;

        int identifier = my_slot + hash;
        String token = null;
        if (config.getNumTokens() == 1)
        {
            logger.info("Trying to createToken with slot {} with rac count {} with rac membership size {} with dc {}",
                    my_slot, membership.getRacCount(), membership.getRacMembershipSize(), config.getDC());
            token = tokenManager.createToken(
                    my_slot, membership.getRacCount(), membership.getRacMembershipSize(), config.getDC());
        }
        return factory.create(config.getAppName(), identifier, config.getInstanceName(), config.getHostname(), config.getHostIP(), config.getRac(), null, token);

    }

    /*
     * @param A map of the rac for each instance.
     */
    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        this.locMap = locMap;
    }

}
