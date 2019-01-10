/*
 * Copyright 2017 Netflix, Inc.
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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.DateUtils;

/** Utility functions for date. Created by aagrawal on 7/10/17. */
@Singleton
public class DateUtil {

    public static final String yyyyMMdd = "yyyyMMdd";
    public static final String yyyyMMddHHmm = "yyyyMMddHHmm";
    private static final String[] patterns = {yyyyMMddHHmm, yyyyMMdd};
    private static final ZoneId defaultZoneId = ZoneId.systemDefault();
    private static final ZoneId utcZoneId = ZoneId.of("UTC");

    /**
     * Format the given date in format yyyyMMdd
     *
     * @param date to format
     * @return date formatted in yyyyMMdd
     */
    public static String formatyyyyMMdd(Date date) {
        if (date == null) return null;
        return DateUtils.formatDate(date, yyyyMMdd);
    }

    /**
     * Format the given date in format yyyyMMddHHmm
     *
     * @param date to format
     * @return date formatted in yyyyMMddHHmm
     */
    public static String formatyyyyMMddHHmm(Date date) {
        if (date == null) return null;
        return DateUtils.formatDate(date, yyyyMMddHHmm);
    }

    /**
     * Format the given date in given format
     *
     * @param date to format
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
        if (StringUtils.isEmpty(date)) return null;
        return DateUtils.parseDate(date, patterns);
    }

    /**
     * Convert date to LocalDateTime using system default zone.
     *
     * @param date Date to be transformed
     * @return converted date to LocalDateTime
     */
    public static LocalDateTime convert(Date date) {
        if (date == null) return null;
        return date.toInstant().atZone(defaultZoneId).toLocalDateTime();
    }

    /**
     * Format the given date in format yyyyMMdd
     *
     * @param date to format
     * @return date formatted in yyyyMMdd
     */
    public static String formatyyyyMMdd(LocalDateTime date) {
        if (date == null) return null;
        return date.format(DateTimeFormatter.ofPattern(yyyyMMdd));
    }

    /**
     * Format the given date in format yyyyMMddHHmm
     *
     * @param date to format
     * @return date formatted in yyyyMMddHHmm
     */
    public static String formatyyyyMMddHHmm(LocalDateTime date) {
        if (date == null) return null;
        return date.format(DateTimeFormatter.ofPattern(yyyyMMddHHmm));
    }

    /**
     * Parse the string to LocalDateTime
     *
     * @param date to parse. Accepted formats are yyyyMMddHHmm and yyyyMMdd
     * @return the parsed LocalDateTime or null if input could not be parsed
     */
    public static LocalDateTime getLocalDateTime(String date) {
        if (StringUtils.isEmpty(date)) return null;

        try {
            LocalDateTime localDateTime =
                    LocalDateTime.parse(date, DateTimeFormatter.ofPattern(yyyyMMddHHmm));
            if (localDateTime != null) return localDateTime;
        } catch (DateTimeParseException e) {
            // Try the date only.
            try {
                LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(yyyyMMdd));
                return localDate.atTime(0, 0);
            } catch (DateTimeParseException ex) {
                return null;
            }
        }

        return null;
    }

    /**
     * Return the current instant
     *
     * @return the instant
     */
    public static Instant getInstant() {
        return Instant.now();
    }

    /**
     * Format the instant based on the pattern passed. If instant or pattern is null, null is
     * returned.
     *
     * @param pattern Pattern that should
     * @param instant Instant in time
     * @return The formatted instant based on the pattern. Null, if pattern or instant is null.
     */
    public static String formatInstant(String pattern, Instant instant) {
        if (instant == null || StringUtils.isEmpty(pattern)) return null;

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern).withZone(utcZoneId);
        return formatter.format(instant);
    }

    /**
     * Parse the dateTime string to Instant based on the predefined set of patterns.
     *
     * @param dateTime DateTime string that needs to be parsed.
     * @return Instant object depicting the date/time.
     */
    public static final Instant parseInstant(String dateTime) {
        LocalDateTime localDateTime = getLocalDateTime(dateTime);
        if (localDateTime == null) return null;
        return localDateTime.atZone(utcZoneId).toInstant();
    }

    public static class DateRange {
        Instant startTime;
        Instant endTime;

        public DateRange(Instant startTime, Instant endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public DateRange(String daterange) {
            if (StringUtils.isBlank(daterange) || daterange.equalsIgnoreCase("default")) {
                endTime = getInstant();
                startTime = endTime.minus(1, ChronoUnit.DAYS);
            } else {
                String[] dates = daterange.split(",");
                startTime = parseInstant(dates[0]);
                endTime = parseInstant(dates[1]);
            }
        }

        public String match() {
            if (startTime == null || endTime == null) return StringUtils.EMPTY;
            String sString = startTime.toEpochMilli() + "";
            String eString = endTime.toEpochMilli() + "";
            int diff = StringUtils.indexOfDifference(sString, eString);
            if (diff < 0) return sString;
            return sString.substring(0, diff);
        }

        public Instant getStartTime() {
            return startTime;
        }

        public Instant getEndTime() {
            return endTime;
        }

        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }

        @Override
        public boolean equals(Object obj) {
            return obj.getClass().equals(this.getClass())
                    && (startTime.toEpochMilli() == ((DateRange) obj).startTime.toEpochMilli())
                    && (endTime.toEpochMilli() == ((DateRange) obj).endTime.toEpochMilli());
        }
    }
}
