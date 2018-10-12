/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.priam.tuner;

import com.netflix.priam.scheduler.UnsupportedTypeException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Garbage collection types supported by Priam for Cassandra (CMS/G1GC). Created by aagrawal on
 * 8/24/17.
 */
public enum GCType {
    CMS("CMS"),
    G1GC("G1GC");

    private static final Logger logger = LoggerFactory.getLogger(GCType.class);
    private final String gcType;

    GCType(String gcType) {
        this.gcType = gcType.toUpperCase();
    }

    /*
     * Helper method to find the garbage colleciton type - case insensitive as user may put value which are not right case.
     * This returns the GCType if one is found. Refer to table below to understand the use-case.
     *
     * GCTypeValue|acceptNullorEmpty|acceptIllegalValue|Result
     * Valid value       |NA               |NA                |GCType
     * Empty string      |True             |NA                |NULL
     * NULL              |True             |NA                |NULL
     * Empty string      |False            |NA                |UnsupportedTypeException
     * NULL              |False            |NA                |UnsupportedTypeException
     * Illegal value     |NA               |True              |NULL
     * Illegal value     |NA               |False             |UnsupportedTypeException
     */

    public static GCType lookup(
            String gcType, boolean acceptNullOrEmpty, boolean acceptIllegalValue)
            throws UnsupportedTypeException {
        if (StringUtils.isEmpty(gcType))
            if (acceptNullOrEmpty) return null;
            else {
                String message =
                        String.format(
                                "%s is not a supported GC Type. Supported values are %s",
                                gcType, getSupportedValues());
                logger.error(message);
                throw new UnsupportedTypeException(message);
            }

        try {
            return GCType.valueOf(gcType.toUpperCase());
        } catch (IllegalArgumentException ex) {
            String message =
                    String.format(
                            "%s is not a supported GCType. Supported values are %s",
                            gcType, getSupportedValues());

            if (acceptIllegalValue) {
                message =
                        message
                                + ". Since acceptIllegalValue is set to True, returning NULL instead.";
                logger.error(message);
                return null;
            }

            logger.error(message);
            throw new UnsupportedTypeException(message, ex);
        }
    }

    private static String getSupportedValues() {
        StringBuilder supportedValues = new StringBuilder();
        boolean first = true;
        for (GCType type : GCType.values()) {
            if (!first) supportedValues.append(",");
            supportedValues.append(type);
            first = false;
        }

        return supportedValues.toString();
    }

    public static GCType lookup(String gcType) throws UnsupportedTypeException {
        return lookup(gcType, false, false);
    }

    public String getGcType() {
        return gcType;
    }
}
