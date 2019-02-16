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
import com.google.inject.Inject;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.TestPropertyOverride;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.PriamHelperFunctions;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({ArchaiusModule.class, BRTestModule.class})
public class StandardTunerTest {
    /* note: these are, more or less, arbitrary partitioner class names. as long as the tests exercise the code, all is good */
    private static final String A_PARTITIONER = "com.netflix.priam.utils.NonexistentPartitioner";
    private static final String RANDOM_PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
    private static final String MURMUR_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
    private static final String BOP_PARTITIONER = "org.apache.cassandra.dht.ByteOrderedPartitioner";
    private final File target = new File("/tmp/priam_test.yaml");
    @Inject private StandardTuner tuner;
    @Inject private InstanceInfo instanceInfo;
    @Inject private PriamHelperFunctions priamHelperFunctions;

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

    @Before
    @After
    public void cleanup() {
        FileUtils.deleteQuietly(target);
    }

    @Test
    public void dump() throws Exception {
        Files.copy(new File("src/main/resources/incr-restore-cassandra.yaml"), target);
        tuner.writeAllProperties(target.getAbsolutePath(), "your_host", "YourSeedProvider");
    }

    @Test
    @TestPropertyOverride({
        "Priam.client_encryption.optional=true",
        "Priam.client_encryption.keystore_password=test",
        "Priam.randomKey=randomKeyValue",
        "Priam.randomGroup.randomKey=randomGroupValue"
    })
    public void addExtraParams() throws Exception {
        String cassParamName1 = "client_encryption_options.optional";
        String priamKeyName1 = "Priam.client_encryption.optional";
        String cassParamName2 = "client_encryption_options.keystore_password";
        String priamKeyName2 = "Priam.client_encryption.keystore_password";
        String cassParamName3 = "randomKey";
        String priamKeyName3 = "Priam.randomKey";
        String cassParamName4 = "randomGroup.randomKey";
        String priamKeyName4 = "Priam.randomGroup.randomKey";

        String extraConfigParam =
                String.format(
                        "%s=%s,%s=%s,%s=%s,%s=%s",
                        priamKeyName1,
                        cassParamName1,
                        priamKeyName2,
                        cassParamName2,
                        priamKeyName3,
                        cassParamName3,
                        priamKeyName4,
                        cassParamName4);

        StandardTuner tuner =
                new StandardTuner(
                        priamHelperFunctions,
                        new TunerConfiguration(extraConfigParam),
                        instanceInfo);
        Files.copy(new File("src/main/resources/incr-restore-cassandra.yaml"), target);
        tuner.writeAllProperties(target.getAbsolutePath(), "your_host", "YourSeedProvider");

        // Read the tuned file and verify
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        Map map = yaml.load(new FileInputStream(target));
        Assert.assertEquals("your_host", map.get("listen_address"));
        Assert.assertEquals("true", ((Map) map.get("client_encryption_options")).get("optional"));
        Assert.assertEquals(
                "test", ((Map) map.get("client_encryption_options")).get("keystore_password"));
        Assert.assertEquals("randomKeyValue", map.get("randomKey"));
        Assert.assertEquals("randomGroupValue", ((Map) map.get("randomGroup")).get("randomKey"));
    }

    private class TunerConfiguration implements IConfiguration {
        String extraConfigParams;

        TunerConfiguration(String extraConfigParam) {
            this.extraConfigParams = extraConfigParam;
        }

        @Override
        public String getExtraConfigParams() {
            return extraConfigParams;
        }
    }
}
