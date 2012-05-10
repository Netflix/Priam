package com.netflix.priam.stream;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.cassandra.io.sstable.SSTableLoaderWrapper;
import org.apache.cassandra.streaming.PendingFile;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.FifoQueue;

public class StreamingTest
{
    public void teststream() throws IOException, InterruptedException
    {
        SSTableLoaderWrapper loader = new SSTableLoaderWrapper(new FakeConfiguration("test", "cass_upg107_ccs", "test", "ins_id"));
        Collection<PendingFile> ssts = loader.stream(new File("/tmp/Keyspace2/"));
        loader.deleteCompleted(ssts);
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

    @Test
    public void testAbstractPath()
    {
        Injector injector = Guice.createInjector(new BRTestModule());
        IConfiguration conf = injector.getInstance(IConfiguration.class);
        InstanceIdentity factory = injector.getInstance(InstanceIdentity.class);

        FifoQueue<AbstractBackupPath> queue = new FifoQueue<AbstractBackupPath>(10);
        for (int i = 10; i < 30; i++)
        {
            S3BackupPath path = new S3BackupPath(conf, factory);
            path.parseRemote("test_backup/fake-region/fakecluster/123456/201108" + i + "0000" + "/SNAP/ks1/f1" + i + ".db");
            queue.adjustAndAdd(path);
        }

        for (int i = 10; i < 30; i++)
        {
            S3BackupPath path = new S3BackupPath(conf, factory);
            path.parseRemote("test_backup/fake-region/fakecluster/123456/201108" + i + "0000" + "/SNAP/ks1/f2" + i + ".db");
            queue.adjustAndAdd(path);
        }

        for (int i = 10; i < 30; i++)
        {
            S3BackupPath path = new S3BackupPath(conf, factory);
            path.parseRemote("test_backup/fake-region/fakecluster/123456/201108" + i + "0000" + "/SNAP/ks1/f3" + i + ".db");
            queue.adjustAndAdd(path);
        }

        S3BackupPath path = new S3BackupPath(conf, factory);
        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108290000" + "/SNAP/ks1/f129.db");
        Assert.assertTrue(queue.contains(path));
        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108290000" + "/SNAP/ks1/f229.db");
        Assert.assertTrue(queue.contains(path));
        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108290000" + "/SNAP/ks1/f329.db");
        Assert.assertTrue(queue.contains(path));

        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108260000/SNAP/ks1/f326.db To: cass/data/ks1/f326.db");
        Assert.assertEquals(path, queue.first());
        // for (AbstractBackupPath p : queue)
        // System.out.println(p);
    }
}
