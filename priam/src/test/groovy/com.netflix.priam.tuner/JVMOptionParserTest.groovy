package com.netflix.priam.tuner

import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 8/28/17.
 */
@Unroll
class JVMOptionParserTest extends Specification{

    def "JMVOptionParsing.toJVMString() for value #jvmOption is #result"() {
        expect:
        JVMOption.parse(jvmOption).toJVMOptionString() == result

        where:
        jvmOption || result
        "#-XX:+PrintGCDetails" || "#-XX:+PrintGCDetails"
        "#-XX:NumberOfGCLogFiles=10" || "#-XX:NumberOfGCLogFiles=10"
        "-XX+UseCMSInitiatingOccupancyOnly" || "-XX+UseCMSInitiatingOccupancyOnly"
        "#-Dcassandra.available_processors=number_of_processors" || "#-Dcassandra.available_processors=number_of_processors"
        "###-XX:+PrintGCDetails" || "#-XX:+PrintGCDetails"
        "#-Dcassandra.join_ring=true|false" || "#-Dcassandra.join_ring=true|false"
    }

    def "JMVOptionParsing for value #jvmOption is #result"(){
        expect:
        JVMOption.parse(jvmOption) == result

        where:
        jvmOption || result
        "### Debug options" || null
        "-XX+UseCMSInitiatingOccupancyOnly" || new JVMOption("-XX+UseCMSInitiatingOccupancyOnly")
        "#-XX:NumberOfGCLogFiles=10" || new JVMOption("-XX:NumberOfGCLogFiles", "10", true, false)
        "-Xms20G" || new JVMOption("-Xms", "20G", false, true)
    }

    def "Heap JVM for value #jvmOption is #result"() {
        expect:
        JVMOption.parse(jvmOption).isHeapJVMOption() == result

        where:
        jvmOption || result
        "#-Xms20G" || true
        "#-Xmx20G" || true
        "#-Xmn20G" || true
        "-Xms20G" || true
        "-Xmx20G" || true
        "-Xmn20G" || true
        "#-Xmdf20G" || false
    }
}
