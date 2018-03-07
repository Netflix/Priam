package com.netflix.priam.cluser.management

import com.netflix.priam.FakeConfiguration
import com.netflix.priam.cluster.management.Compaction
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test class to verify that compaction columnfamily is translated correctly to Map,
 * Created by aagrawal on 2/26/18.
 */
@Unroll
public class TestCompaction extends Specification {
    def "Map contains KS #keyspace with configuration #compactionCFList is #result"() {
        expect:
        Compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList), null).containsKey(keyspace) == result

        where:
        compactionCFList | keyspace || result
        "abc.*"          | "abc"    || true
        "abc.*,def.*"    | "abc"    || true
        "abc.*,def.*"    | "def"    || true
        "abc.def"        | "abc"    || true
        "abc.*,def.*"    | "abc1"   || false
        "abc.*,def.*"    | "def1"   || false
    }

    def "Map contains KS #keyspace, CF #columnfamily with configuration #compactionCFList is #result"() {
        expect:
        Compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList), null).get(keyspace).contains(columnfamily) == result

        where:
        compactionCFList | keyspace | columnfamily || result
        "abc.*,def.*"    | "abc"    | "column1"    || false
        "abc.*,def.*"    | "def"    | "dude"       || false
        "abc.def"        | "abc"    | "def"        || true
        "abc.*,def.ghi"    | "def"    | "ghi"       || true
        "abc.def"        | "abc"    | "ghi"        || false
    }

    def "Map contains KS #keyspace, with configuration #compactionCFList is empty"() {
        expect:
        Compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList), null).get(keyspace).isEmpty() == result

        where:
        compactionCFList | keyspace || result
        "abc.*"          | "abc"    || true
        "abc.*,def.*"    | "abc"    || true
        "abc.*,def.*"    | "def"    || true
    }

    def "Exception with configuration #compactionCFList"() {
        when:
        Compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList), null)

        then:
        thrown(expectedException)

        where:
        compactionCFList || expectedException
        "abc"            || IllegalArgumentException
        "abc,def"        || IllegalArgumentException
        "abc.*,def"      || IllegalArgumentException
        "abc,def.*"      || IllegalArgumentException
    }


    private class CompactionConfiguration extends FakeConfiguration {
        private String compactionCFList;

        CompactionConfiguration(String compactionCFList) {
            this.compactionCFList = compactionCFList;
        }

        @Override
        public String getCompactionCFList() {
            return compactionCFList;
        }

    }
}
