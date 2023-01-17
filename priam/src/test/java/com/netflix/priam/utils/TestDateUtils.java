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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 11/29/18. */
public class TestDateUtils {

    @Test
    public void testDateRangeDefault() {
        String input = "default";
        Instant now = DateUtil.getInstant();
        DateUtil.DateRange dateRange = new DateUtil.DateRange(input);

        // Start and end should be a day apart.
        Assert.assertEquals(
                dateRange.getEndTime(), dateRange.getStartTime().plus(1, ChronoUnit.DAYS));
        if (Duration.between(dateRange.getEndTime(), now).getSeconds() > 5)
            throw new AssertionError(
                    String.format(
                            "End date: %s and now: %s should be almost same",
                            dateRange.getEndTime(), now));
    }

    @Test
    public void testDateRangeEmpty() {
        Instant now = DateUtil.getInstant();
        DateUtil.DateRange dateRange = new DateUtil.DateRange("     ");

        // Start and end should be a day apart.
        Assert.assertEquals(
                dateRange.getEndTime(), dateRange.getStartTime().plus(1, ChronoUnit.DAYS));
        if (Duration.between(dateRange.getEndTime(), now).getSeconds() > 5)
            throw new AssertionError(
                    String.format(
                            "End date: %s and now: %s should be almost same",
                            dateRange.getEndTime(), now));
    }

    @Test
    public void testDateRangeValues() {
        String start = "201801011201";
        String end = "201801051201";
        DateUtil.DateRange dateRange = new DateUtil.DateRange(start + "," + end);
        Assert.assertEquals(Instant.ofEpochSecond(1514808060), dateRange.getStartTime());
        Assert.assertEquals(Instant.ofEpochSecond(1515153660), dateRange.getEndTime());

        start = "20180101";
        end = "20180105";
        dateRange = new DateUtil.DateRange(start + "," + end);
        Assert.assertEquals(Instant.ofEpochSecond(1514764800), dateRange.getStartTime());
        Assert.assertEquals(Instant.ofEpochSecond(1515110400), dateRange.getEndTime());
    }

    @Test
    public void testDateRangeRandom() {
        DateUtil.DateRange dateRange = new DateUtil.DateRange("some,random,values");
        Assert.assertEquals(null, dateRange.getStartTime());
        Assert.assertEquals(null, dateRange.getEndTime());
    }

    @Test
    public void testDateRangeMatch() {
        Instant dateStart = Instant.ofEpochMilli(1543632497000L);
        Instant dateEnd = Instant.ofEpochMilli(1543819697000L);
        DateUtil.DateRange dateRange = new DateUtil.DateRange(dateStart, dateEnd);
        Assert.assertEquals("1543", dateRange.match());

        dateRange = new DateUtil.DateRange(dateStart, null);
        Assert.assertEquals(StringUtils.EMPTY, dateRange.match());
    }

    @Test
    public void testFutureDateRangeValues() {
        String start = "202801011201";
        String end = "202801051201";
        DateUtil.DateRange dateRange = new DateUtil.DateRange(start + "," + end);
        Assert.assertEquals(Instant.ofEpochSecond(1830340860), dateRange.getStartTime());
        Assert.assertEquals(Instant.ofEpochSecond(1830686460), dateRange.getEndTime());
        Assert.assertEquals("1830", dateRange.match());
    }
}
