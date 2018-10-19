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
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadTokenRetriever extends TokenRetrieverBase implements IDeadTokenRetriever {
    private static final Logger logger = LoggerFactory.getLogger(DeadTokenRetriever.class);
    private final IPriamInstanceFactory factory;
    private final IMembership membership;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private String
            replacedIp; // The IP address of the dead instance to which we will acquire its token
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

        if (logger.isInfoEnabled()) {
            if (instanceInfo.getInstanceEnvironment() == InstanceInfo.InstanceEnvironment.CLASSIC) {
                logger.info(
                        "EC2 classic instances (local ASG): "
                                + Arrays.toString(asgInstances.toArray()));
                logger.info(
                        "VPC Account (cross-account ASG): "
                                + Arrays.toString(crossAccountAsgInstances.toArray()));
            } else {
                logger.info("VPC Account (local ASG): " + Arrays.toString(asgInstances.toArray()));
                logger.info(
                        "EC2 classic instances (cross-account ASG): "
                                + Arrays.toString(crossAccountAsgInstances.toArray()));
            }
        }

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
        final List<PriamInstance> allIds = factory.getAllIds(config.getAppName());
        List<String> asgInstances = membership.getRacMembership();
        if (config.isDualAccount()) {
            asgInstances = getDualAccountRacMembership(asgInstances);
        } else {
            logger.info("Single Account cluster");
        }

        // Sleep random interval - upto 15 sec
        sleeper.sleep(new Random().nextInt(5000) + 10000);
        for (PriamInstance dead : allIds) {
            // test same zone and is it is alive.
            if (!dead.getRac().equals(instanceInfo.getRac())
                    || asgInstances.contains(dead.getInstanceId())
                    || super.isInstanceDummy(dead)) continue;
            logger.info("Found dead instances: {}", dead.getInstanceId());
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

            // find the replaced IP
            this.replacedIp = findReplaceIp(allIds, markAsDead.getToken(), markAsDead.getDC());
            if (this.replacedIp == null) this.replacedIp = markAsDead.getHostIP();

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
    public String getReplaceIp() {
        return this.replacedIp;
    }

    private String findReplaceIp(List<PriamInstance> allIds, String token, String location) {
        String ip;
        for (PriamInstance ins : allIds) {
            logger.info("Calling getIp on hostname[{}] and token[{}]", ins.getHostName(), token);
            if (ins.getToken().equals(token) || !ins.getDC().equals(location)) {
                // avoid using dead instance and other regions instances
                continue;
            }

            try {
                ip = getIp(ins.getHostName(), token);
            } catch (ParseException e) {
                ip = null;
            }

            if (ip != null) {
                logger.info("Found the IP: {}", ip);
                return ip;
            }
        }

        return null;
    }

    private String getIp(String host, String token) throws ParseException {
        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        String baseURI = getBaseURI(host);
        WebResource service = client.resource(baseURI);

        ClientResponse clientResp;
        String textEntity;

        try {
            clientResp =
                    service.path("Priam/REST/v1/cassadmin/gossipinfo")
                            .accept(MediaType.APPLICATION_JSON)
                            .get(ClientResponse.class);

            if (clientResp.getStatus() != 200) return null;

            textEntity = clientResp.getEntity(String.class);

            logger.info(
                    "Respond from calling gossipinfo on host[{}] and token[{}] : {}",
                    host,
                    token,
                    textEntity);

            if (StringUtils.isEmpty(textEntity)) return null;
        } catch (Exception e) {
            logger.info("Error in reaching out to host: {}", baseURI);
            return null;
        }

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(textEntity);

        JSONObject jsonObject = (JSONObject) obj;

        for (Object key : jsonObject.keySet()) {
            JSONObject msg = (JSONObject) jsonObject.get(key);
            if (msg.get("Token") == null) {
                continue;
            }
            String tokenVal = (String) msg.get("Token");

            if (token.equals(tokenVal)) {
                logger.info(
                        "Using gossipinfo from host[{}] and token[{}], the replaced address is : {}",
                        host,
                        token,
                        key);
                return (String) key;
            }
        }
        return null;
    }

    private String getBaseURI(String host) {
        return "http://" + host + ":8080/";
    }

    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        this.locMap = locMap;
    }
}
