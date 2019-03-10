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
package com.netflix.priam.cluser.management

import com.google.inject.Guice
import com.netflix.priam.backup.BRTestModule
import com.netflix.priam.cluster.management.Compaction
import com.netflix.priam.config.FakeConfiguration
import com.netflix.priam.connection.CassandraOperations
import com.netflix.priam.health.CassandraMonitor
import mockit.Mock
import mockit.MockUp
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Test class to verify that compaction columnfamily is translated correctly to Map,
 * Created by aagrawal on 2/26/18.
 */
@Unroll
class TestCompaction extends Specification {
    @Shared
    private static Compaction compaction


    def setup(){
        new MockCassandraOperations()

    }
    def setupSpec(){
        if (compaction == null)
            compaction = Guice.createInjector(new BRTestModule()).getInstance(Compaction.class)
    }

    def "Map contains KS #keyspace with configuration #compactionCFIncludeList is #result"() {
        expect:
        compaction.getCompactionIncludeFilter(new CompactionConfiguration(compactionCFIncludeList, null)).containsKey(keyspace) == result

        where:
        compactionCFIncludeList | keyspace || result
        "abc.*"                 | "abc"    || true
        "abc.*,def.*"           | "abc"    || true
        "abc.*,def.*"           | "def"    || true
        "abc.def"               | "abc"    || true
        "abc.*,def.*"           | "abc1"   || false
        "abc.*,def.*"           | "def1"   || false
    }

    def "Map contains KS #keyspace, CF #columnfamily with configuration #compactionCFIncludeList is #result"() {
        expect:
        compaction.getCompactionIncludeFilter(new CompactionConfiguration(compactionCFIncludeList, null)).get(keyspace).contains(columnfamily) == result

        where:
        compactionCFIncludeList | keyspace | columnfamily || result
        "abc.*,def.*"           | "abc"    | "column1"    || false
        "abc.*,def.*"           | "def"    | "dude"       || false
        "abc.def"               | "abc"    | "def"        || true
        "abc.*,def.ghi"         | "def"    | "ghi"       || true
        "abc.def"               | "abc"    | "ghi"        || false
    }

    def "Map contains KS #keyspace, with configuration #compactionCFIncludeList is empty"() {
        expect:
        compaction.getCompactionIncludeFilter(new CompactionConfiguration(compactionCFIncludeList, null)).get(keyspace).isEmpty() == result

        where:
        compactionCFIncludeList | keyspace || result
        "abc.*"                 | "abc"    || true
        "abc.*,def.*"           | "abc"    || true
        "abc.*,def.*"           | "def"    || true
    }

    def "Map contains KS #keyspace, CF #columnfamily with include config #compactionCFIncludeList and exclude config #compactionCFExcludeList is #result"() {
        expect:
        compaction.getCompactionFilterCfs(new CompactionConfiguration(compactionCFIncludeList, compactionCFExcludeList)).get(keyspace).contains(columnfamily) == result

        where:
        compactionCFIncludeList | compactionCFExcludeList | keyspace | columnfamily || result
        "abc.*,def.*"           | "def.*"                 | "abc"    | "column1"    || true
        "abc.*,def.*"           | "def.*"                 | "abc"    | "column2"    || false
        "def.*"                 | "def.ghi"               | "def"    | "dude"       || true
        "def.*"                 | "def.ghi"               | "def"    | "ghi"        || false
        null                    | null                    | "def"    | "dude"       || true
        null                    | "def.ghi"               | "def"    | "ghi"        || false
        null                    | "def.ghi"               | "def"    | "dude"       || true
        null                    | "def.ghi"               | "def"    | "random"     || false
        null                    | "def.ghi"               | "def"    | "dude"        || true
        "abc.*,def.*"           | null                    | "abc"    | "column1"    || true
        "abc.column1"           | null                    | "abc"    | "column1"    || true
    }

    def "Map contains KS #keyspace with include config #compactionCFIncludeList and exclude config #compactionCFExcludeList is #result"() {
        expect:
        compaction.getCompactionFilterCfs(new CompactionConfiguration(compactionCFIncludeList, compactionCFExcludeList)).containsKey(keyspace) == result

        where:
        compactionCFIncludeList | compactionCFExcludeList | keyspace || result
        "abc.column2"           | null                    | "abc"    || false
        "abc.column2"           | null                    | "def"    || false
        "abc.*,def.*"           | "def.*"                 | "def"    || false
        null                    | null                    | "system"    || false

    }

        def "Exception with configuration #compactionCFIncludeList"() {
        when:
        compaction.getCompactionIncludeFilter(new CompactionConfiguration(compactionCFIncludeList, null))

        then:
        thrown(expectedException)

        where:
        compactionCFIncludeList || expectedException
        "abc"                   || IllegalArgumentException
        "abc,def"               || IllegalArgumentException
        "abc.*,def"             || IllegalArgumentException
        "abc,def.*"             || IllegalArgumentException
    }

    def testConcurrentCompaction() throws Exception{
        expect:
        concurrentRuns(size) == result

        where:
        size || result
        3 || 2
        7 || 6
        1 || 0
    }

    private static int concurrentRuns(int size) {
        CassandraMonitor.setIsCassadraStarted()
        ExecutorService threads = Executors.newFixedThreadPool(size)
        List<Callable<Boolean>> torun = new ArrayList<>(size)
        for (int i = 0; i < size; i++) {
            torun.add(new Callable<Boolean>() {
                Boolean call() throws Exception {
                    compaction.execute()
                    return Boolean.TRUE
                }
            })
        }

        // all tasks executed in different threads, at 'once'.
        List<Future<Boolean>> futures = threads.invokeAll(torun)

        // no more need for the threadpool
        threads.shutdown()
        // check the results of the tasks.
        int noOfBadRun = 0
        for (Future<Boolean> fut : futures) {
            //We expect exception here.
            try{
                fut.get()
            }catch(Exception ignored){
                noOfBadRun++
            }
        }

        return noOfBadRun
    }



    private class CompactionConfiguration extends FakeConfiguration {
        private String compactionCFIncludeList
        private String compactionCFExcludeList

        CompactionConfiguration(String compactionCFIncludeList, String compactionCFExcludeList) {
            this.compactionCFIncludeList = compactionCFIncludeList
            this.compactionCFExcludeList = compactionCFExcludeList
        }

        @Override
        String getCompactionIncludeCFList() {
            return compactionCFIncludeList
        }

        @Override
        String getCompactionExcludeCFList(){
            return compactionCFExcludeList
        }

    }


    private static class MockCassandraOperations extends MockUp<CassandraOperations> {
        @Mock
        static void forceKeyspaceCompaction(String keyspaceName, String... columnfamily) throws Exception{
            Thread.sleep(2000)
        }

        @Mock
        static Map<String,List<String>> getColumnfamilies() throws Exception{
            Map<String, List<String>> result = new HashMap<>()
            result.put("abc", Arrays.asList("column1"))
            result.put("def", Arrays.asList("dude", "ghi"))
            result.put("ghi", Arrays.asList("k1", "ghi", "k2"))
            result.put("system", Arrays.asList("compaction_history", "hints"))
            return result
        }
    }
}
