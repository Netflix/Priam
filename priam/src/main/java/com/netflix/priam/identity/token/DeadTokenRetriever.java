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
import com.netflix.priam.utils.SystemUtils;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
            // TODO: If instance is in SHUTTING_DOWN mode, it might not show up in asg instances (if
            // cloud control plane is having issues), thus, we should not try to replace the
            // instance as it will lead to "Cannot replace a live node" issue.

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
            this.replacedIp =
                    findReplaceIp(
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
                Thread.currentThread().sleep(sleepTime);
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

    private String findReplaceIp(List<PriamInstance> allIds, String token, String dc)
            throws Exception {
        // Avoid using dead instance who we are trying to replace (duh!!)
        // Avoid other regions instances to avoid communication over public ip address.
        List<PriamInstance> eligibleInstances =
                allIds.parallelStream()
                        .filter(priamInstance -> !priamInstance.getToken().equalsIgnoreCase(token))
                        .filter(priamInstance -> priamInstance.getDC().equalsIgnoreCase(dc))
                        .collect(Collectors.toList());
        // We want to get IP from min 1, max 3 instances to ensure we are not relying on gossip of a
        // single instance.
        // Good idea to shuffle so we are not talking to same instances every time.
        Collections.shuffle(eligibleInstances);
        // Potential issue could be when you have about 50% of your cluster C* DOWN or trying to be
        // replaced.
        // Think of a major disaster hitting your cluster. In that scenario chances of instance
        // hitting DOWN C* are much much higher.
        // In such a case you should rely on @link{CassandraConfig#setReplacedIp}.
        int noOfInstancesGossipShouldMatch = Math.max(1, Math.min(3, eligibleInstances.size()));
        int noOfInstancesWithGossipMatch = 0;
        String replace_ip = null, ip = null;
        for (PriamInstance ins : eligibleInstances) {
            logger.info("Calling getIp on hostname[{}] and token[{}]", ins.getHostName(), token);
            ip = getIp(ins.getHostName(), token);
            if (StringUtils.isEmpty(replace_ip)) replace_ip = ip;
            if (!StringUtils.isEmpty(replace_ip) && !StringUtils.isEmpty(ip)) {
                if (replace_ip.equalsIgnoreCase(ip)) {
                    noOfInstancesWithGossipMatch++;
                    if (noOfInstancesWithGossipMatch >= noOfInstancesGossipShouldMatch) {
                        logger.info(
                                "Using replace_ip: {} as # of required gossip info match: {}",
                                replace_ip,
                                noOfInstancesGossipShouldMatch);
                        return replace_ip;
                    }
                } else
                    throw new Exception(
                            String.format(
                                    "Unexpected Exception: Gossip info from hosts are not matching: found {} and {}",
                                    replace_ip,
                                    ip));
            }
        }
        logger.warn(
                "Return null: Unable to find enough instances where gossip match. Required: {}",
                noOfInstancesGossipShouldMatch);
        return null;
    }

    private String getIp(String host, String token) {
        String response = null;
        try {
            response = SystemUtils.getDataFromUrl(getGossipInfoURL(host));

            String inputToken = String.format("[%s]", token);
            JSONParser parser = new JSONParser();
            JSONArray jsonObject = (JSONArray) parser.parse(response);

            for (Object key : jsonObject) {
                JSONObject msg = (JSONObject) key;

                // Ensure that we are not trying to replace a NORMAL token and token of that
                // instance matches what we want to replace.
                if (msg.get("STATUS") == null
                        || msg.get("STATUS").toString().equalsIgnoreCase("NORMAL")
                        || msg.get("TOKENS") == null
                        || msg.get("PUBLIC_IP") == null
                        || !msg.get("TOKENS").toString().equals(inputToken)) {
                    continue;
                }

                logger.info(
                        "Using gossip info from host[{}] and token[{}], the replaced address is : [{}]",
                        host,
                        token,
                        msg.get("PUBLIC_IP"));
                return (String) msg.get("PUBLIC_IP");
            }
        } catch (Exception e) {
            logger.info(
                    "Error in reaching out to host: [{}} or parsing response from host: {}",
                    host,
                    response);
        }
        return null;
    }

    private String getGossipInfoURL(String host) {
        return "http://" + host + ":8080/Priam/REST/v1/cassadmin/gossipinfo";
    }

    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        this.locMap = locMap;
    }
}
