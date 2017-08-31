package com.netflix.priam.tuner

import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 8/28/17.
 */
@Unroll
class JVMOptionParserTest extends Specification{

    def "JMVOptionParsing for value #jvmOption is #result"() {
        expect:
        JVMOption.parse(jvmOption).jvmString() == result

        where:
        jvmOption || result
        "#-XX:+PrintGCDetails" || "#-XX:+PrintGCDetails"
        "#-XX:NumberOfGCLogFiles=10" || "#-XX:NumberOfGCLogFiles=10"
        "-XX+UseCMSInitiatingOccupancyOnly" || "-XX+UseCMSInitiatingOccupancyOnly"
        "#-Dcassandra.available_processors=number_of_processors" || "#-Dcassandra.available_processors=number_of_processors"
        "###-XX:+PrintGCDetails" || "#-XX:+PrintGCDetails"
        "#-Dcassandra.join_ring=true|false" || "#-Dcassandra.join_ring=true|false"
        "### Debug options" || null
    }
}
