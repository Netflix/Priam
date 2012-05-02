package com.netflix.priam.stream;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.cassandra.io.sstable.SSTableLoaderWrapper;
import org.apache.cassandra.streaming.PendingFile;
import org.junit.Test;

import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.utils.FifoQueue;

public class StreamingTest
{
    public void teststream() throws IOException, InterruptedException
    {
        SSTableLoaderWrapper loader = new SSTableLoaderWrapper(new FakeConfiguration("test", "cass_upg107_ccs", "test", "ins_id"));
        Collection<PendingFile> ssts = loader.stream(new File("/tmp/Keyspace2/"));
        loader.deleteCompleted(ssts);
    }

    public static void main(String[] args) throws IOException, InterruptedException
    {
        new StreamingTest().teststream();
        System.exit(0);
    }
    
    @Test
    public void testFifoAddAndRemove()
    {
        FifoQueue<Long> queue = new FifoQueue<Long>(10);
        for (long i = 0; i < 100; i++)
            queue.adjustAndAdd(i);
        Assert.assertEquals(10, queue.size());
        Assert.assertEquals(new Long(90), queue.first());
    }
}
