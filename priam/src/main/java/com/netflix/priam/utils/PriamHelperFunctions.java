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

package com.netflix.priam.utils;

import com.google.inject.Inject;
import com.netflix.archaius.api.PropertyRepository;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by aagrawal on 10/29/18. */
public class PriamHelperFunctions {
    private static final Logger logger = LoggerFactory.getLogger(PriamHelperFunctions.class);
    private final PropertyRepository propertyRepository;

    @Inject
    public PriamHelperFunctions(PropertyRepository propertyRepository) {
        this.propertyRepository = propertyRepository;
    }

    public void parseParams(Map map, String envParams) {
        if (StringUtils.isEmpty(envParams) || map == null) {
            return;
        }

        Arrays.stream(envParams.split(","))
                .forEach(
                        pair1 -> {
                            String[] pair = pair1.split("=");
                            if (pair.length > 1) {
                                String priamKey = pair[0];
                                String cassKey = pair[1];
                                String cassVal = getProperty(priamKey, String.class);

                                if (!StringUtils.isBlank(cassKey)
                                        && !StringUtils.isBlank(cassVal)) {
                                    if (!cassKey.contains(".")) {
                                        logger.info(
                                                "Updating yaml: PriamKey: [{}], Key: [{}], OldValue: [{}], NewValue: [{}]",
                                                priamKey,
                                                cassKey,
                                                map.get(cassKey),
                                                cassVal);
                                        map.put(cassKey, cassVal);
                                    } else {
                                        // split the cassandra key. We will get the group and get
                                        // the key name.
                                        String[] cassKeySplit = cassKey.split("\\.");
                                        Map cassKeyMap =
                                                ((Map)
                                                        map.getOrDefault(
                                                                cassKeySplit[0], new HashMap()));
                                        map.putIfAbsent(cassKeySplit[0], cassKeyMap);
                                        logger.info(
                                                "Updating yaml: PriamKey: [{}], Key: [{}], OldValue: [{}], NewValue: [{}]",
                                                priamKey,
                                                cassKey,
                                                cassKeyMap.get(cassKeySplit[1]),
                                                cassVal);
                                        cassKeyMap.put(cassKeySplit[1], cassVal);
                                    }
                                }
                            }
                        });
    }

    public <T> T getProperty(String var1, Class<T> var2) {
        return propertyRepository.get(var1, var2).get();
    }
}
