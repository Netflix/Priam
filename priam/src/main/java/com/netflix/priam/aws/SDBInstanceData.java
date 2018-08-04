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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredential;
import com.netflix.priam.identity.PriamInstance;

import java.util.*;

/**
 * DAO for handling Instance identity information such as token, zone, region
 */
@Singleton
public class SDBInstanceData {
    public static class Attributes {
        public final static String APP_ID = "appId";
        public final static String ID = "id";
        public final static String INSTANCE_ID = "instanceId";
        public final static String TOKEN = "token";
        public final static String AVAILABILITY_ZONE = "availabilityZone";
        public final static String ELASTIC_IP = "elasticIP";
        public final static String UPDATE_TS = "updateTimestamp";
        public final static String LOCATION = "location";
        public final static String HOSTNAME = "hostname";
    }

    public static final String DOMAIN = "InstanceIdentity";
    public static final String ALL_QUERY = "select * from " + DOMAIN + " where " + Attributes.APP_ID + "='%s'";
    public static final String INSTANCE_QUERY = "select * from " + DOMAIN + " where " + Attributes.APP_ID + "='%s' and " + Attributes.LOCATION + "='%s' and " + Attributes.ID + "='%d'";

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
        SelectRequest request = new SelectRequest(String.format(INSTANCE_QUERY, app, dc, id));
        SelectResult result = simpleDBClient.select(request);
        if (result.getItems().size() == 0)
            return null;
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
        Set<PriamInstance> inslist = new HashSet<PriamInstance>();
        String nextToken = null;
        do {
            SelectRequest request = new SelectRequest(String.format(ALL_QUERY, app));
            request.setNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            Iterator<Item> itemiter = result.getItems().iterator();
            while (itemiter.hasNext()) {
                inslist.add(transform(itemiter.next()));
            }

        } while (nextToken != null);
        return inslist;
    }

    /**
     * Create a new instance entry in SimpleDB
     *
     * @param instance
     * @throws AmazonServiceException
     */
    public void createInstance(PriamInstance instance) throws AmazonServiceException {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq = new PutAttributesRequest(DOMAIN, getKey(instance), createAttributesToRegister(instance));
        simpleDBClient.putAttributes(putReq);
    }

    /**
     * Register a new instance. Registration will fail if a prior entry exists
     *
     * @param instance
     * @throws AmazonServiceException
     */
    public void registerInstance(PriamInstance instance) throws AmazonServiceException {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq = new PutAttributesRequest(DOMAIN, getKey(instance), createAttributesToRegister(instance));
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
        DeleteAttributesRequest delReq = new DeleteAttributesRequest(DOMAIN, getKey(instance), createAttributesToDeRegister(instance));
        simpleDBClient.deleteAttributes(delReq);
    }

    protected List<ReplaceableAttribute> createAttributesToRegister(PriamInstance instance) {
        instance.setUpdatetime(new Date().getTime());
        List<ReplaceableAttribute> attrs = new ArrayList<ReplaceableAttribute>();
        attrs.add(new ReplaceableAttribute(Attributes.INSTANCE_ID, instance.getInstanceId(), false));
        if (instance.getToken() != null) {
            attrs.add(new ReplaceableAttribute(Attributes.TOKEN, instance.getToken(), true));
        }
        attrs.add(new ReplaceableAttribute(Attributes.APP_ID, instance.getApp(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ID, Integer.toString(instance.getId()), true));
        attrs.add(new ReplaceableAttribute(Attributes.AVAILABILITY_ZONE, instance.getRac(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ELASTIC_IP, instance.getHostIP(), true));
        attrs.add(new ReplaceableAttribute(Attributes.HOSTNAME, instance.getHostName(), true));
        attrs.add(new ReplaceableAttribute(Attributes.LOCATION, instance.getDC(), true));
        attrs.add(new ReplaceableAttribute(Attributes.UPDATE_TS, Long.toString(instance.getUpdatetime()), true));
        return attrs;
    }

    protected List<Attribute> createAttributesToDeRegister(PriamInstance instance) {
        List<Attribute> attrs = new ArrayList<Attribute>();
        attrs.add(new Attribute(Attributes.INSTANCE_ID, instance.getInstanceId()));
        if (instance.getToken() != null) {
            attrs.add(new Attribute(Attributes.TOKEN, instance.getToken()));
        }
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
        Iterator<Attribute> attrs = item.getAttributes().iterator();
        while (attrs.hasNext()) {
            Attribute att = attrs.next();
            if (att.getName().equals(Attributes.INSTANCE_ID))
                ins.setInstanceId(att.getValue());
            else if (att.getName().equals(Attributes.TOKEN))
                ins.setToken(att.getValue());
            else if (att.getName().equals(Attributes.APP_ID))
                ins.setApp(att.getValue());
            else if (att.getName().equals(Attributes.ID))
                ins.setId(Integer.parseInt(att.getValue()));
            else if (att.getName().equals(Attributes.AVAILABILITY_ZONE))
                ins.setRac(att.getValue());
            else if (att.getName().equals(Attributes.ELASTIC_IP))
                ins.setHostIP(att.getValue());
            else if (att.getName().equals(Attributes.HOSTNAME))
                ins.setHost(att.getValue());
            else if (att.getName().equals(Attributes.LOCATION))
                ins.setDC(att.getValue());
            else if (att.getName().equals(Attributes.UPDATE_TS))
                ins.setUpdatetime(Long.parseLong(att.getValue()));
        }
        return ins;
    }

    private String getKey(PriamInstance instance) {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getId();
    }

    private AmazonSimpleDB getSimpleDBClient() {
        //Create per request
        return AmazonSimpleDBClient.builder().withCredentials(provider.getAwsCredentialProvider()).withRegion(configuration.getSDBInstanceIdentityRegion()).build();
    }
}
