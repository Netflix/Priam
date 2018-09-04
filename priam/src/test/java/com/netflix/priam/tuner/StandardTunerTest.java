/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.priam.tuner;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Properties;

import com.google.common.io.Files;
import com.netflix.priam.config.FakeConfiguration;
import com.netflix.priam.config.IConfiguration;
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

    private StandardTuner tuner;
    private IConfiguration config;

    @Before
    public void setup()
    {

        config = new FakeConfiguration();
        tuner = new StandardTuner(config);
        File targetDir = new File(config.getCassConfigurationDirectory());
        if(!targetDir.exists())
            targetDir.mkdirs();
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
    public void dump() throws Exception {
        String target = "/tmp/priam_test.yaml";
        Files.copy(new File("src/main/resources/incr-restore-cassandra.yaml"), new File("/tmp/priam_test.yaml"));
        tuner.writeAllProperties(target, "your_host", "YourSeedProvider");
    }

    @Test
    public void testPropertiesFiles() throws Exception
    {
        FakeConfiguration fake = (FakeConfiguration) config;
        String target = "/tmp/priam_test.yaml";
        Files.copy(new File("src/main/resources/incr-restore-cassandra.yaml"), new File(target));

        File testRackDcFile = new File("src/test/resources/conf/cassandra-rackdc.properties");
        File rackDcFile = new File(Paths.get(config.getCassConfigurationDirectory(), "cassandra-rackdc.properties").normalize().toString());
        Files.copy(testRackDcFile, rackDcFile);

        try {
            fake.fakeProperties.put("propertyOverrides.cassandra-rackdc", "dc=${dc},rack=${rac},ec2_naming_scheme=legacy,dc_suffix=testsuffix");

            tuner.writeAllProperties(target, "your_host", "YourSeedProvider");
            Properties prop = new Properties();
            prop.load(new FileReader(rackDcFile));
            assertEquals("us-east-1", prop.getProperty("dc"));
            assertEquals("my_zone", prop.getProperty("rack"));
            assertEquals("legacy", prop.getProperty("ec2_naming_scheme"));
            assertEquals("testsuffix", prop.getProperty("dc_suffix"));

            assertEquals(4, prop.stringPropertyNames().size());
        } finally {
            fake.fakeProperties.clear();
            for (String line : Files.readLines(rackDcFile, Charset.defaultCharset()))
            {
                System.out.println(line);
            }

            Files.copy(testRackDcFile, rackDcFile);
        }
    }
}
