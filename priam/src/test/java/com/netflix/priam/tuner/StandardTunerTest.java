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

package com.netflix.priam.tuner;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Files;
import com.google.inject.Guice;
import com.netflix.priam.backup.BRTestModule;
import java.io.File;
import org.junit.Test;

public class StandardTunerTest {
    /* note: these are, more or less, arbitrary partitioner class names. as long as the tests exercise the code, all is good */
    private static final String A_PARTITIONER = "com.netflix.priam.utils.NonexistentPartitioner";
    private static final String RANDOM_PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
    private static final String MURMUR_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
    private static final String BOP_PARTITIONER = "org.apache.cassandra.dht.ByteOrderedPartitioner";

    private final StandardTuner tuner;

    public StandardTunerTest() {
        this.tuner = Guice.createInjector(new BRTestModule()).getInstance(StandardTuner.class);
    }

    @Test
    public void derivePartitioner_NullYamlEntry() {
        String partitioner = tuner.derivePartitioner(null, A_PARTITIONER);
        assertEquals(A_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_EmptyYamlEntry() {
        String partitioner = tuner.derivePartitioner("", A_PARTITIONER);
        assertEquals(A_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_RandomPartitioner() {
        String partitioner = tuner.derivePartitioner(RANDOM_PARTITIONER, RANDOM_PARTITIONER);
        assertEquals(RANDOM_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_MurmurPartitioner() {
        String partitioner = tuner.derivePartitioner(MURMUR_PARTITIONER, MURMUR_PARTITIONER);
        assertEquals(MURMUR_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_BOPPartitionerInYaml() {
        String partitioner = tuner.derivePartitioner(BOP_PARTITIONER, MURMUR_PARTITIONER);
        assertEquals(BOP_PARTITIONER, partitioner);
    }

    @Test
    public void derivePartitioner_BOPPartitionerInConfig() {
        String partitioner = tuner.derivePartitioner(RANDOM_PARTITIONER, BOP_PARTITIONER);
        assertEquals(BOP_PARTITIONER, partitioner);
    }

    @Test
    public void dump() throws Exception {
        String target = "/tmp/priam_test.yaml";
        Files.copy(
                new File("src/main/resources/incr-restore-cassandra.yaml"),
                new File("/tmp/priam_test.yaml"));
        tuner.writeAllProperties(target, "your_host", "YourSeedProvider");
    }
}
