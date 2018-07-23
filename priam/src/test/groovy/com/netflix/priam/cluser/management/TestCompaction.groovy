package com.netflix.priam.cluser.management

import com.google.common.collect.ImmutableList
import com.google.inject.Guice
import com.netflix.priam.FakeConfiguration
import com.netflix.priam.TestModule
import com.netflix.priam.backup.BRTestModule
import com.netflix.priam.cluster.management.Compaction
import com.netflix.priam.defaultimpl.CassandraOperations
import mockit.Mock
import mockit.MockUp
import org.junit.Assert
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.inject.Inject
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Test class to verify that compaction columnfamily is translated correctly to Map,
 * Created by aagrawal on 2/26/18.
 */
@Unroll
public class TestCompaction extends Specification {
    @Shared
    private static Compaction compaction;

    def setup(){
        new MockCassandraOperations();

    }
    def setupSpec(){
        if (compaction == null)
            compaction = Guice.createInjector(new BRTestModule()).getInstance(Compaction.class);
    }

    def "Map contains KS #keyspace with configuration #compactionCFList is #result"() {
        expect:
        compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList)).containsKey(keyspace) == result

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
        compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList)).get(keyspace).contains(columnfamily) == result

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
        compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList)).get(keyspace).isEmpty() == result

        where:
        compactionCFList | keyspace || result
        "abc.*"          | "abc"    || true
        "abc.*,def.*"    | "abc"    || true
        "abc.*,def.*"    | "def"    || true
    }

    def "Exception with configuration #compactionCFList"() {
        when:
        compaction.updateCompactionCFList(new CompactionConfiguration(compactionCFList))

        then:
        thrown(expectedException)

        where:
        compactionCFList || expectedException
        "abc"            || IllegalArgumentException
        "abc,def"        || IllegalArgumentException
        "abc.*,def"      || IllegalArgumentException
        "abc,def.*"      || IllegalArgumentException
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

    private int concurrentRuns(int size) {
        ExecutorService threads = Executors.newFixedThreadPool(size);
        List<Callable<Boolean>> torun = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            torun.add(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    compaction.execute();
                    return Boolean.TRUE;
                }
            });
        }

        // all tasks executed in different threads, at 'once'.
        List<Future<Boolean>> futures = threads.invokeAll(torun);

        // no more need for the threadpool
        threads.shutdown();
        // check the results of the tasks.
        int noOfBadRun = 0;
        for (Future<Boolean> fut : futures) {
            //We expect exception here.
            try{
                fut.get();
            }catch(Exception e){
                noOfBadRun++
            }
        }

        return noOfBadRun;
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


    private static class MockCassandraOperations extends MockUp<CassandraOperations> {
        @Mock
        public void forceKeyspaceCompaction(String keyspaceName, String columnfamily) throws Exception{
            Thread.sleep(2000);
        }

        @Mock
        public List<String> getKeyspaces() throws Exception{
            return ImmutableList.of("system", "hello");
        }
    }
}
