package com.netflix.priam.stream;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import com.netflix.priam.TestAmazonConfiguration;
import com.netflix.priam.TestBackupConfiguration;
import com.netflix.priam.TestCassandraConfiguration;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import junit.framework.Assert;

import org.apache.cassandra.io.sstable.SSTableLoaderWrapper;
import org.apache.cassandra.streaming.PendingFile;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.IncrementalRestore;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.FifoQueue;

public class StreamingTest
{
    public void teststream() throws IOException, InterruptedException
    {
        Injector injector = Guice.createInjector(new BRTestModule());
        CassandraConfiguration cassandraConfiguration = injector.getInstance(TestCassandraConfiguration.class);
        AmazonConfiguration amazonConfiguration = injector.getInstance(TestAmazonConfiguration.class);
        BackupConfiguration backupConfiguration = injector.getInstance(TestBackupConfiguration.class);

        SSTableLoaderWrapper loader = new SSTableLoaderWrapper(cassandraConfiguration, backupConfiguration, amazonConfiguration);
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
        CassandraConfiguration cassandraConfiguration = new TestCassandraConfiguration("fake-app");
        AmazonConfiguration amazonConfiguration = new TestAmazonConfiguration("fake-app", "fake-region", "az1", "fakeInstance1");
        BackupConfiguration backupConfiguration = new TestBackupConfiguration();
        InstanceIdentity factory = injector.getInstance(InstanceIdentity.class);

        FifoQueue<AbstractBackupPath> queue = new FifoQueue<AbstractBackupPath>(10);
        for (int i = 10; i < 30; i++)
        {
            S3BackupPath path = new S3BackupPath(cassandraConfiguration, amazonConfiguration, backupConfiguration, factory);
            path.parseRemote("test_backup/fake-region/fakecluster/123456/201108" + i + "0000" + "/SNAP/ks1/f1" + i + ".db");
            queue.adjustAndAdd(path);
        }

        for (int i = 10; i < 30; i++)
        {
            S3BackupPath path = new S3BackupPath(cassandraConfiguration, amazonConfiguration, backupConfiguration, factory);
            path.parseRemote("test_backup/fake-region/fakecluster/123456/201108" + i + "0000" + "/SNAP/ks1/f2" + i + ".db");
            queue.adjustAndAdd(path);
        }

        for (int i = 10; i < 30; i++)
        {
            S3BackupPath path = new S3BackupPath(cassandraConfiguration, amazonConfiguration, backupConfiguration, factory);
            path.parseRemote("test_backup/fake-region/fakecluster/123456/201108" + i + "0000" + "/SNAP/ks1/f3" + i + ".db");
            queue.adjustAndAdd(path);
        }

        S3BackupPath path = new S3BackupPath(cassandraConfiguration, amazonConfiguration, backupConfiguration, factory);
        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108290000" + "/SNAP/ks1/f129.db");
        Assert.assertTrue(queue.contains(path));
        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108290000" + "/SNAP/ks1/f229.db");
        Assert.assertTrue(queue.contains(path));
        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108290000" + "/SNAP/ks1/f329.db");
        Assert.assertTrue(queue.contains(path));

        path.parseRemote("test_backup/fake-region/fakecluster/123456/201108260000/SNAP/ks1/f326.db To: cass/data/ks1/f326.db");
        Assert.assertEquals(path, queue.first());
    }

    @Test
    public void testIgnoreIndexFiles()
    {
        String[] testInputs = new String[] { "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Digest.sha1",
                "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Filter.db", "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Data.db",
                "User_Authentication_Audit.User_Authentication_Audit_appkey_idx-hc-93-Statistics.db", "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Filter.db",
                "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Digest.sha1", "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Statistics.db", "CS_Agents.CS_Agents_supervisorEmpSk_idx-hc-1-Data.db" };
        Assert.assertFalse(IncrementalRestore.SECONDRY_INDEX_PATTERN.matcher("cfname_test_name_idx-hc.db").matches());
        for (String input : testInputs)
            Assert.assertTrue(IncrementalRestore.SECONDRY_INDEX_PATTERN.matcher(input).matches());
    }

}
