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

package com.netflix.priam.cluser.management

import com.netflix.priam.cluster.management.Flush
import com.netflix.priam.config.FakeConfiguration
import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 7/15/17.
 */
@Unroll
class TestFlushTask extends Specification {
    def "Exception for value #flushCronExpression"() {
        when:
        Flush.getTimer(new FlushConfiguration(flushCronExpression))

        then:
        thrown(expectedException)

        where:
       flushCronExpression || expectedException
        "abc"              || IllegalArgumentException
       null                || IllegalArgumentException
    }

    def "SchedulerType for value #flushSchedulerType, #flushCronExpression, #flushInterval is null"() {
        expect:
        Flush.getTimer(new FlushConfiguration(flushCronExpression)) == result

        where:
        flushCronExpression || result
        "-1"                || null
    }

    def "SchedulerType for value #flushCronExpression is #result"() {
        expect:
        Flush.getTimer(new FlushConfiguration(flushCronExpression)).getCronExpression() == result

        where:
        flushCronExpression || result
        "0 0 0/1 1/1 * ? *" || "0 0 0/1 1/1 * ? *"
    }

    private class FlushConfiguration extends FakeConfiguration {
        private String flushCronExpression

        FlushConfiguration(String flushCronExpression) {
            this.flushCronExpression = flushCronExpression
        }

        @Override
        String getFlushCronExpression() {
            return flushCronExpression
        }
    }
}