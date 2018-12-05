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
        Assert.assertEquals(DateUtil.getDate(start).toInstant(), dateRange.getStartTime());
        Assert.assertEquals(DateUtil.getDate(end).toInstant(), dateRange.getEndTime());
    }

    @Test
    public void testDateRangeRandom() {
        DateUtil.DateRange dateRange = new DateUtil.DateRange("some,random,values");
        Assert.assertEquals(null, dateRange.getStartTime());
        Assert.assertEquals(null, dateRange.getEndTime());
    }
}
