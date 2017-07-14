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

package com.netflix.priam.utils;


import org.apache.http.client.utils.DateUtils;

import javax.inject.Singleton;
import java.util.Date;

/**
 * Utility functions for date.
 * Created by aagrawal on 7/10/17.
 */
@Singleton
public class DateUtil {

    private final static String yyyyMMdd = "yyyyMMdd";
    private final static String yyyyMMddHHmm = "yyyyMMddHHmm";
    private final static String[] patterns = {yyyyMMddHHmm, yyyyMMdd};

    /**
     * Format the given date in format yyyyMMdd
     *
     * @param date to format
     * @return date formatted in yyyyMMdd
     */
    public static String formatyyyyMMdd(Date date) {
        return DateUtils.formatDate(date, yyyyMMdd);
    }

    /**
     * Format the given date in format yyyyMMddHHmm
     *
     * @param date to format
     * @return date formatted in yyyyMMddHHmm
     */
    public static String formatyyyyMMddHHmm(Date date) {
        return DateUtils.formatDate(date, yyyyMMddHHmm);
    }

    /**
     * Format the given date in given format
     *
     * @param date    to format
     * @param pattern e.g. yyyyMMddHHmm
     * @return formatted date
     */
    public static String formatDate(Date date, String pattern) {
        return DateUtils.formatDate(date, pattern);
    }

    /**
     * Parse the string to date
     *
     * @param date to parse. Accepted formats are yyyyMMddHHmm and yyyyMMdd
     * @return the parsed date or null if input could not be parsed
     */
    public static Date getDate(String date) {
        return DateUtils.parseDate(date, patterns);
    }

}
