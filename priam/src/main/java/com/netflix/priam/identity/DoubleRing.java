/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.identity;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.ITokenManager;
import java.io.*;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class providing functionality for doubling the ring */
public class DoubleRing {
    private static final Logger logger = LoggerFactory.getLogger(DoubleRing.class);
    private static File TMP_BACKUP_FILE;
    private final IConfiguration config;
    private final IPriamInstanceFactory<PriamInstance> factory;
    private final ITokenManager tokenManager;
    private final InstanceInfo instanceInfo;

    @Inject
    public DoubleRing(
            IConfiguration config,
            IPriamInstanceFactory factory,
            ITokenManager tokenManager,
            InstanceInfo instanceInfo) {
        this.config = config;
        this.factory = factory;
        this.tokenManager = tokenManager;
        this.instanceInfo = instanceInfo;
    }

    /**
     * Doubling is done by pre-calculating all slots of a double ring and registering them. When new
     * nodes come up, they will get the unsed token assigned per token logic.
     */
    public void doubleSlots() {
        List<PriamInstance> local = filteredRemote(factory.getAllIds(config.getAppName()));

        // delete all
        for (PriamInstance data : local) factory.delete(data);

        int hash = tokenManager.regionOffset(instanceInfo.getRegion());
        // move existing slots.
        for (PriamInstance data : local) {
            int slot = (data.getId() - hash) * 2;
            factory.create(
                    data.getApp(),
                    hash + slot,
                    data.getInstanceId(),
                    data.getHostName(),
                    data.getHostIP(),
                    data.getRac(),
                    data.getVolumes(),
                    data.getToken());
        }

        int new_ring_size = local.size() * 2;
        for (PriamInstance data : filteredRemote(factory.getAllIds(config.getAppName()))) {
            // if max then rotate.
            int currentSlot = data.getId() - hash;
            int new_slot =
                    currentSlot + 3 > new_ring_size
                            ? (currentSlot + 3) - new_ring_size
                            : currentSlot + 3;
            String token =
                    tokenManager.createToken(new_slot, new_ring_size, instanceInfo.getRegion());
            factory.create(
                    data.getApp(),
                    new_slot + hash,
                    InstanceIdentity.DUMMY_INSTANCE_ID,
                    instanceInfo.getHostname(),
                    config.usePrivateIP() ? instanceInfo.getPrivateIP() : instanceInfo.getHostIP(),
                    data.getRac(),
                    null,
                    token);
        }
    }

    // filter other DC's
    private List<PriamInstance> filteredRemote(List<PriamInstance> lst) {
        List<PriamInstance> local = Lists.newArrayList();
        for (PriamInstance data : lst)
            if (data.getDC().equals(instanceInfo.getRegion())) local.add(data);
        return local;
    }

    /** Backup the current state in case of failure */
    public void backup() throws IOException {
        // writing to the backup file.
        TMP_BACKUP_FILE = File.createTempFile("Backup-instance-data", ".dat");
        try (ObjectOutputStream stream =
                new ObjectOutputStream(new FileOutputStream(TMP_BACKUP_FILE))) {
            stream.writeObject(filteredRemote(factory.getAllIds(config.getAppName())));
            logger.info(
                    "Wrote the backup of the instances to: {}", TMP_BACKUP_FILE.getAbsolutePath());
        }
    }

    /**
     * Restore tokens if a failure occurs
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void restore() throws IOException, ClassNotFoundException {
        for (PriamInstance data : filteredRemote(factory.getAllIds(config.getAppName())))
            factory.delete(data);

        // read from the file.
        try (ObjectInputStream stream =
                new ObjectInputStream(new FileInputStream(TMP_BACKUP_FILE))) {
            @SuppressWarnings("unchecked")
            List<PriamInstance> allInstances = (List<PriamInstance>) stream.readObject();
            for (PriamInstance data : allInstances)
                factory.create(
                        data.getApp(),
                        data.getId(),
                        data.getInstanceId(),
                        data.getHostName(),
                        data.getHostIP(),
                        data.getRac(),
                        data.getVolumes(),
                        data.getToken());
            logger.info(
                    "Successfully restored the Instances from the backup: {}",
                    TMP_BACKUP_FILE.getAbsolutePath());
        }
    }
}
