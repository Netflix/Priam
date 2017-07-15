package com.netflix.priam.scheduler

import com.netflix.priam.FakeConfiguration
import com.netflix.priam.cluster.management.FlushTask
import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 7/15/17.
 */
@Unroll
class TestFlushTask extends Specification {

    def "Exception for value #flushSchedulerType, #flushCronExpression, #flushInterval"() {
        when:
        FlushTask.getTimer(new FlushConfiguration(flushSchedulerType, flushCronExpression, flushInterval))

        then:
        def error = thrown(expectedException)

        where:
        flushSchedulerType | flushCronExpression | flushInterval    || expectedException
        "sdf"              | null                | null             || UnsupportedTypeException
        "hour"             | null                | "2"              || IllegalArgumentException
        "hour"             | "0 0 2 * * ?"       | "2"              || IllegalArgumentException
        "cron"             | "abc"               | null             || Exception
        "cron"             | "abc"               | "daily=2"        || Exception
        "hour"             | null                | "hour=2,daily=2" || IllegalArgumentException
    }

    def "SchedulerType for value #flushSchedulerType, #flushCronExpression, #flushInterval is null"() {
        expect:
        FlushTask.getTimer(new FlushConfiguration(flushSchedulerType, flushCronExpression, flushInterval)) == result

        where:
        flushSchedulerType | flushCronExpression | flushInterval || result
        "hour"             | null                | null          || null
        "cron"             | null                | null          || null
        "hour"             | "abc"               | null          || null
        "cron"             | null                | "abc"         || null
    }

    def "SchedulerType for value #flushSchedulerType, #flushCronExpression, #flushInterval is #result"() {
        expect:
        FlushTask.getTimer(new FlushConfiguration(flushSchedulerType, flushCronExpression, flushInterval)).getCronExpression() == result

        where:
        flushSchedulerType | flushCronExpression | flushInterval || result
        "hour"             | null                | "daily=2"     || "0 0 2 * * ?"
        "hour"             | null                | "hour=2"      || "0 2 0/1 * * ?"
        "cron"             | "0 0 0/1 1/1 * ? *" | null          || "0 0 0/1 1/1 * ? *"
        "cron"             | "0 0 0/1 1/1 * ? *" | "daily=2"     || "0 0 0/1 1/1 * ? *"
    }


    private class FlushConfiguration extends FakeConfiguration {
        private String flushSchedulerType, flushCronExpression, flushInterval;

        FlushConfiguration(String flushSchedulerType, String flushCronExpression, String flushInterval) {
            this.flushCronExpression = flushCronExpression;
            this.flushSchedulerType = flushSchedulerType;
            this.flushInterval = flushInterval;
        }

        @Override
        public SchedulerType getFlushSchedulerType() throws UnsupportedTypeException {
            return SchedulerType.lookup(flushSchedulerType);
        }

        @Override
        public String getFlushCronExpression() {
            return flushCronExpression;
        }

        @Override
        public String getFlushInterval() {
            return flushInterval;
        }
    }

}
