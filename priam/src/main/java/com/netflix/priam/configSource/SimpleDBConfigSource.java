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
package com.netflix.priam.configSource;

import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.priam.cred.ICredential;
import java.util.Iterator;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads config data from SimpleDB. {@link #initialize(String, String)} will query the SimpleDB
 * domain "PriamProperties" for any potential configurations. The domain is set up to support
 * multiple different clusters; this is done by using amazon's auto scaling groups (ASG).
 *
 * <p>Schema
 *
 * <ul>
 *   <li>"appId" // ASG up to first instance of '-'. So ASG name priam-test will create appId priam,
 *       ASG priam_test will create appId priam_test.
 *   <li>"property" // key to use for configs.
 *   <li>"value" // value to set for the given property/key.
 *   <li>"region" // region the config belongs to. If left empty, then applies to all regions.
 * </ul>
 *
 * }
 */
public final class SimpleDBConfigSource extends AbstractConfigSource {
    private static final Logger logger =
            LoggerFactory.getLogger(SimpleDBConfigSource.class.getName());

    private static final String DOMAIN = "PriamProperties";

    private final Map<String, String> data = Maps.newConcurrentMap();
    private final ICredential provider;

    @Inject
    public SimpleDBConfigSource(final ICredential provider) {
        this.provider = provider;
    }

    @Override
    public void initialize(final String asgName, final String region) {
        super.initialize(asgName, region);

        // End point is us-east-1
        AmazonSimpleDB simpleDBClient =
                AmazonSimpleDBClient.builder()
                        .withCredentials(provider.getAwsCredentialProvider())
                        .build();

        String nextToken = null;
        String appid =
                asgName.lastIndexOf('-') > 0 ? asgName.substring(0, asgName.indexOf('-')) : asgName;
        logger.info("appid used to fetch properties is: {}", appid);
        do {
            String ALL_QUERY = "select * from " + DOMAIN + " where " + Attributes.APP_ID + "='%s'";
            SelectRequest request = new SelectRequest(String.format(ALL_QUERY, appid));
            request.setNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            for (Item item : result.getItems()) addProperty(item);

        } while (nextToken != null);
    }

    private static class Attributes {
        public static final String APP_ID = "appId"; // ASG
        public static final String PROPERTY = "property";
        public static final String PROPERTY_VALUE = "value";
        public static final String REGION = "region";
    }

    private void addProperty(Item item) {
        Iterator<Attribute> attrs = item.getAttributes().iterator();
        String prop = "";
        String value = "";
        String dc = "";
        while (attrs.hasNext()) {
            Attribute att = attrs.next();
            if (att.getName().equals(Attributes.PROPERTY)) prop = att.getValue();
            else if (att.getName().equals(Attributes.PROPERTY_VALUE)) value = att.getValue();
            else if (att.getName().equals(Attributes.REGION)) dc = att.getValue();
        }
        // Ignore, if not this region
        if (StringUtils.isNotBlank(dc) && !dc.equals(getRegion())) return;
        // Override only if region is specified
        if (data.containsKey(prop) && StringUtils.isBlank(dc)) return;
        data.put(prop, value);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public String get(final String key) {
        return data.get(key);
    }

    @Override
    public void set(final String key, final String value) {
        Preconditions.checkNotNull(value, "Value can not be null for configurations.");
        data.put(key, value);
    }
}
