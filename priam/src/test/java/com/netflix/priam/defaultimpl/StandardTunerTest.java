package com.netflix.priam.defaultimpl;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.IConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StandardTunerTest
{
    /* note: these are, more or less, arbitrary paritioner class names. as long as the tests exercise the code, all is good */
    private static final String A_PARTITIONER = "com.netflix.priam.utils.NonexistentPartitioner";
    private static final String RANDOM_PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
    private static final String MURMUR_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
    private static final String BOP_PARTITIONER = "org.apache.cassandra.dht.ByteOrderedPartitioner";

    private IConfiguration config;
    private StandardTuner tuner;

    @Before
    public void setup()
    {

        config = new FakeConfiguration();
        tuner = new StandardTuner(config);
    }

    @Test
    public void derivePartitioner_NullYamlEntry()
    {
        String partitioner = tuner.derivePartitioner(null, A_PARTITIONER);
        assertEquals(A_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_EmptyYamlEntry()
    {
        String partitioner = tuner.derivePartitioner("", A_PARTITIONER);
        assertEquals(A_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_RandomPartitioner()
    {
        String partitioner = tuner.derivePartitioner(RANDOM_PARTITIONER, RANDOM_PARTITIONER);
        assertEquals(RANDOM_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_MurmurPartitioner()
    {
        String partitioner = tuner.derivePartitioner(MURMUR_PARTITIONER, MURMUR_PARTITIONER);
        assertEquals(MURMUR_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_BOPPartitionerInYaml()
    {
        String partitioner = tuner.derivePartitioner(BOP_PARTITIONER, MURMUR_PARTITIONER);
        assertEquals(BOP_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_BOPPartitionerInConfig()
    {
        String partitioner = tuner.derivePartitioner(RANDOM_PARTITIONER, BOP_PARTITIONER);
        assertEquals(BOP_PARTITIONER, partitioner);
    }

    @Test
    public void dump() throws IOException
    {
        String target = "/tmp/priam_test.yaml";
        String rackPropertiesPath = config.getRackDcPropertiesLocation();

        Files.copy(new File("src/main/resources/incr-restore-cassandra.yaml"), new File("/tmp/priam_test.yaml"));
        Files.copy(new File("src/test/resources/cassandra-rackdc.properties"), new File(rackPropertiesPath));

        tuner.writeAllProperties(target, "your_host", "YourSeedProvider");
    }
}
