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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Created by aagrawal on 10/29/18. */
public class PriamHelperFunctions {
    private final PropertyRepository propertyRepository;

    @Inject
    public PriamHelperFunctions(PropertyRepository propertyRepository) {
        this.propertyRepository = propertyRepository;
    }

    public Map<String, String> parseParams(String envParams, boolean substitueFromConfig) {
        if (StringUtils.isEmpty(envParams)) {
            return Collections.EMPTY_MAP;
        }

        Map<String, String> extraEnvParamsMap = new HashMap<>();
        Arrays.stream(envParams.split(","))
                .forEach(
                        pair1 -> {
                            String[] pair = pair1.split("=");
                            if (pair.length > 1) {
                                String key, value;

                                if (substitueFromConfig) {
                                    key = pair[1];
                                    value = getProperty(pair[0], String.class);
                                } else {
                                    key = pair[0];
                                    value = pair[1];
                                }

                                if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
                                    extraEnvParamsMap.put(key, value);
                                }
                            }
                        });
        return extraEnvParamsMap;
    }

    public <T> T getProperty(String var1, Class<T> var2) {
        return propertyRepository.get(var1, var2).get();
    }
}
