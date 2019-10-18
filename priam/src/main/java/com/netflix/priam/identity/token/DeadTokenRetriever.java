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

public class DeadTokenRetriever extends TokenRetrieverBase implements IDeadTokenRetriever {
    private static final Logger logger = LoggerFactory.getLogger(DeadTokenRetriever.class);
    private final IPriamInstanceFactory factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final Sleeper sleeper;
    // The IP address of the dead instance to which we will acquire its token
    private String replacedIp;
    private ListMultimap<String, PriamInstance> locMap;
    private final InstanceInfo instanceInfo;

    @Inject
    public DeadTokenRetriever(
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

    private List<String> getDualAccountRacMembership(List<String> asgInstances) {
        logger.info("Dual Account cluster");

        List<String> crossAccountAsgInstances = membership.getCrossAccountRacMembership();

        // Remove duplicates (probably there are not)
        asgInstances.removeAll(crossAccountAsgInstances);

        // Merge the two lists
        asgInstances.addAll(crossAccountAsgInstances);
        logger.info("Combined Instances in the AZ: {}", asgInstances);

        return asgInstances;
    }

    @Override
    public PriamInstance get() throws Exception {

        logger.info("Looking for a token from any dead node");
        final List<PriamInstance> allInstancesWithinCluster =
                factory.getAllIds(config.getAppName());
        List<String> asgInstances = membership.getRacMembership();
        if (config.isDualAccount()) {
            asgInstances = getDualAccountRacMembership(asgInstances);
        } else {
            logger.info("Single Account cluster");
        }

        // Sleep random interval - upto 15 sec
        sleeper.sleep(new Random().nextInt(5000) + 10000);
        logger.info(
                "About to iterate through all instances within cluster "
                        + config.getAppName()
                        + " to find an available token to acquire.");

        for (PriamInstance priamInstance : allInstancesWithinCluster) {
            // test same zone and is it alive.
            if (!priamInstance.getRac().equals(instanceInfo.getRac())
                    || asgInstances.contains(priamInstance.getInstanceId())
                    || super.isInstanceDummy(priamInstance)) continue;
            // TODO: If instance is in SHUTTING_DOWN mode, it might not show up in asg
            // instances (if cloud control plane is having issues), thus, we should not try
            // to replace the instance as it will lead to "Cannot replace a live node"
            // issue.

            logger.info("Found dead instance: {}", priamInstance.toString());

            PriamInstance markAsDead =
                    factory.create(
                            priamInstance.getApp() + "-dead",
                            priamInstance.getId(),
                            priamInstance.getInstanceId(),
                            priamInstance.getHostName(),
                            priamInstance.getHostIP(),
                            priamInstance.getRac(),
                            priamInstance.getVolumes(),
                            priamInstance.getToken());
            // remove it as we marked it down...
            factory.delete(priamInstance);

            // find the replaced IP
            try {
                replacedIp =
                        TokenRetrieverUtils.inferTokenOwnerFromGossip(
                                allInstancesWithinCluster,
                                priamInstance.getToken(),
                                priamInstance.getDC());

                // Lets not replace the instance if gossip info is not merging!!
                if (replacedIp == null) return null;
                logger.info(
                        "Will try to replace token: {} with replacedIp (from gossip info): {} instead of ip from Token database: {}",
                        priamInstance.getToken(),
                        replacedIp,
                        priamInstance.getHostIP());
            } catch (TokenRetrieverUtils.GossipParseException e) {
                // In case of gossip exception, fallback to IP in token database.
                this.replacedIp = priamInstance.getHostIP();
                logger.info(
                        "Will try to replace token: {} with replacedIp from Token database: {}",
                        priamInstance.getToken(),
                        priamInstance.getHostIP());
            }

            PriamInstance result;
            try {
                result =
                        factory.create(
                                config.getAppName(),
                                markAsDead.getId(),
                                instanceInfo.getInstanceId(),
                                instanceInfo.getHostname(),
                                instanceInfo.getHostIP(),
                                instanceInfo.getRac(),
                                markAsDead.getVolumes(),
                                markAsDead.getToken());
            } catch (Exception ex) {
                long sleepTime = super.getSleepTime();
                logger.warn(
                        "Exception when acquiring dead token: "
                                + priamInstance.getToken()
                                + " , will sleep for "
                                + sleepTime
                                + " millisecs before we retry.");
                Thread.sleep(sleepTime);

                throw ex;
            }

            logger.info(
                    "Acquired token: "
                            + priamInstance.getToken()
                            + " and we will replace with replacedIp: "
                            + replacedIp);

            return result;
        }

        logger.info("This node was NOT able to acquire any dead token");
        return null;
    }

    @Override
    public String getReplaceIp() {
        return this.replacedIp;
    }

    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        this.locMap = locMap;
    }
}
