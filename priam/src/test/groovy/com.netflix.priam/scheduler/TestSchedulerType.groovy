package com.netflix.priam.scheduler

/**
 * Created by aagrawal on 3/16/17.
 * This is used to test SchedulerType with all the values you might get.
 */
import spock.lang.*

@Unroll
class TestSchedulerType extends Specification{

    def "Exception for value #schedulerType , #acceptNullorEmpty , #acceptIllegalValue"() {
        when:
        SchedulerType.lookup(schedulerType, acceptNullorEmpty, acceptIllegalValue)

        then:
        def error = thrown(expectedException)

        where:
        schedulerType  | acceptNullorEmpty | acceptIllegalValue || expectedException
        "sdf"          | true              | false              || UnsupportedTypeException
        ""             | false             | true               || UnsupportedTypeException
        null           | false             | true               || UnsupportedTypeException

    }

    def "SchedulerType for value #schedulerType , #acceptNullorEmpty , #acceptIllegalValue is #result"() {
        expect:
        SchedulerType.lookup(schedulerType, acceptNullorEmpty, acceptIllegalValue) == result

        where:
        schedulerType | acceptNullorEmpty | acceptIllegalValue || result
        "hour"        | true              | true               || SchedulerType.HOUR
        "Hour"        | true              | true               || SchedulerType.HOUR
        "HOUR"        | true              | true               || SchedulerType.HOUR
        "hour"        | true              | false              || SchedulerType.HOUR
        "Hour"        | true              | false              || SchedulerType.HOUR
        "HOUR"        | true              | false              || SchedulerType.HOUR
        "hour"        | false             | false              || SchedulerType.HOUR
        "Hour"        | false             | false              || SchedulerType.HOUR
        "HOUR"        | false             | false              || SchedulerType.HOUR
        "hour"        | false             | true               || SchedulerType.HOUR
        "Hour"        | false             | true               || SchedulerType.HOUR
        "HOUR"        | false             | true               || SchedulerType.HOUR
        ""            | true              | false              || null
        null          | true              | false              || null
        "sdf"         | false             | true               || null
        "sdf"         | true              | true               || null
    }


}