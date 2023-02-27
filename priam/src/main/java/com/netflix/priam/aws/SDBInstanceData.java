/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredential;
import com.netflix.priam.identity.PriamInstance;
import java.util.*;
import javax.inject.Inject;
import javax.inject.Singleton;

/** DAO for handling Instance identity information such as token, zone, region */
@Singleton
public class SDBInstanceData {
    public static class Attributes {
        public static final String APP_ID = "appId";
        public static final String ID = "id";
        public static final String INSTANCE_ID = "instanceId";
        public static final String TOKEN = "token";
        public static final String AVAILABILITY_ZONE = "availabilityZone";
        public static final String ELASTIC_IP = "elasticIP";
        public static final String UPDATE_TS = "updateTimestamp";
        public static final String LOCATION = "location";
        public static final String HOSTNAME = "hostname";
    }

    public static final String DOMAIN = "InstanceIdentity";
    public static final String ALL_QUERY =
            "select * from " + DOMAIN + " where " + Attributes.APP_ID + "='%s'";
    public static final String INSTANCE_QUERY =
            "select * from "
                    + DOMAIN
                    + " where "
                    + Attributes.APP_ID
                    + "='%s' and "
                    + Attributes.LOCATION
                    + "='%s' and "
                    + Attributes.ID
                    + "='%d'";

    private final ICredential provider;
    private final IConfiguration configuration;

    @Inject
    public SDBInstanceData(ICredential provider, IConfiguration configuration) {
        this.provider = provider;
        this.configuration = configuration;
    }

    /**
     * Get the instance details from SimpleDB
     *
     * @param app Cluster name
     * @param id Node ID
     * @return the node with the given {@code id}, or {@code null} if no such node exists
     */
    public PriamInstance getInstance(String app, String dc, int id) {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        SelectRequest request =
                new SelectRequest(String.format(INSTANCE_QUERY, app, dc, id))
                        .withConsistentRead(true);
        SelectResult result = simpleDBClient.select(request);
        if (result.getItems().size() == 0) return null;
        return transform(result.getItems().get(0));
    }

    /**
     * Get the set of all nodes in the cluster
     *
     * @param app Cluster name
     * @return the set of all instances in the given {@code app}
     */
    public Set<PriamInstance> getAllIds(String app) {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        Set<PriamInstance> inslist = new HashSet<>();
        String nextToken = null;
        do {
            SelectRequest request =
                    new SelectRequest(String.format(ALL_QUERY, app))
                            .withConsistentRead(true)
                            .withNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            for (Item item : result.getItems()) {
                inslist.add(transform(item));
            }

        } while (nextToken != null);
        return inslist;
    }

    /**
     * Create a new instance entry in SimpleDB
     *
     * @param orig Original instance used for validation
     * @param inst Instance entry to be created.
     * @throws AmazonServiceException If unable to write to Simple DB because of any error.
     */
    public void updateInstance(PriamInstance orig, PriamInstance inst)
            throws AmazonServiceException {
        PutAttributesRequest putReq =
                new PutAttributesRequest(DOMAIN, getKey(inst), createAttributesToRegister(inst))
                        .withExpected(
                                new UpdateCondition()
                                        .withName(Attributes.INSTANCE_ID)
                                        .withValue(orig.getInstanceId()))
                        .withExpected(
                                new UpdateCondition()
                                        .withName(Attributes.TOKEN)
                                        .withValue(orig.getToken()));
        getSimpleDBClient().putAttributes(putReq);
    }

    /**
     * Register a new instance. Registration will fail if a prior entry exists
     *
     * @param instance Instance entry to be registered.
     * @throws AmazonServiceException If unable to write to Simple DB because of any error.
     */
    public void registerInstance(PriamInstance instance) throws AmazonServiceException {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq =
                new PutAttributesRequest(
                        DOMAIN, getKey(instance), createAttributesToRegister(instance));
        UpdateCondition expected = new UpdateCondition();
        expected.setName(Attributes.INSTANCE_ID);
        expected.setExists(false);
        putReq.setExpected(expected);
        simpleDBClient.putAttributes(putReq);
    }

    /**
     * Deregister instance (same as delete)
     *
     * @param instance
     * @throws AmazonServiceException
     */
    public void deregisterInstance(PriamInstance instance) throws AmazonServiceException {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        DeleteAttributesRequest delReq =
                new DeleteAttributesRequest(
                        DOMAIN, getKey(instance), createAttributesToDeRegister(instance));
        simpleDBClient.deleteAttributes(delReq);
    }

    protected List<ReplaceableAttribute> createAttributesToRegister(PriamInstance instance) {
        instance.setUpdatetime(new Date().getTime());
        List<ReplaceableAttribute> attrs = new ArrayList<>();
        attrs.add(
                new ReplaceableAttribute(Attributes.INSTANCE_ID, instance.getInstanceId(), false));
        attrs.add(new ReplaceableAttribute(Attributes.TOKEN, instance.getToken(), true));
        attrs.add(new ReplaceableAttribute(Attributes.APP_ID, instance.getApp(), true));
        attrs.add(
                new ReplaceableAttribute(Attributes.ID, Integer.toString(instance.getId()), true));
        attrs.add(new ReplaceableAttribute(Attributes.AVAILABILITY_ZONE, instance.getRac(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ELASTIC_IP, instance.getHostIP(), true));
        attrs.add(new ReplaceableAttribute(Attributes.HOSTNAME, instance.getHostName(), true));
        attrs.add(new ReplaceableAttribute(Attributes.LOCATION, instance.getDC(), true));
        attrs.add(
                new ReplaceableAttribute(
                        Attributes.UPDATE_TS, Long.toString(instance.getUpdatetime()), true));
        return attrs;
    }

    protected List<Attribute> createAttributesToDeRegister(PriamInstance instance) {
        List<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute(Attributes.INSTANCE_ID, instance.getInstanceId()));
        attrs.add(new Attribute(Attributes.TOKEN, instance.getToken()));
        attrs.add(new Attribute(Attributes.APP_ID, instance.getApp()));
        attrs.add(new Attribute(Attributes.ID, Integer.toString(instance.getId())));
        attrs.add(new Attribute(Attributes.AVAILABILITY_ZONE, instance.getRac()));
        attrs.add(new Attribute(Attributes.ELASTIC_IP, instance.getHostIP()));
        attrs.add(new Attribute(Attributes.HOSTNAME, instance.getHostName()));
        attrs.add(new Attribute(Attributes.LOCATION, instance.getDC()));
        attrs.add(new Attribute(Attributes.UPDATE_TS, Long.toString(instance.getUpdatetime())));
        return attrs;
    }

    /**
     * Convert a simpledb item to PriamInstance
     *
     * @param item
     * @return
     */
    public PriamInstance transform(Item item) {
        PriamInstance ins = new PriamInstance();
        for (Attribute att : item.getAttributes()) {
            if (att.getName().equals(Attributes.INSTANCE_ID)) ins.setInstanceId(att.getValue());
            else if (att.getName().equals(Attributes.TOKEN)) ins.setToken(att.getValue());
            else if (att.getName().equals(Attributes.APP_ID)) ins.setApp(att.getValue());
            else if (att.getName().equals(Attributes.ID))
                ins.setId(Integer.parseInt(att.getValue()));
            else if (att.getName().equals(Attributes.AVAILABILITY_ZONE)) ins.setRac(att.getValue());
            else if (att.getName().equals(Attributes.ELASTIC_IP)) ins.setHostIP(att.getValue());
            else if (att.getName().equals(Attributes.HOSTNAME)) ins.setHost(att.getValue());
            else if (att.getName().equals(Attributes.LOCATION)) ins.setDC(att.getValue());
            else if (att.getName().equals(Attributes.UPDATE_TS))
                ins.setUpdatetime(Long.parseLong(att.getValue()));
        }
        return ins;
    }

    private String getKey(PriamInstance instance) {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getId();
    }

    private AmazonSimpleDB getSimpleDBClient() {
        // Create per request
        return AmazonSimpleDBClient.builder()
                .withCredentials(provider.getAwsCredentialProvider())
                .withRegion(configuration.getSDBInstanceIdentityRegion())
                .build();
    }
}
