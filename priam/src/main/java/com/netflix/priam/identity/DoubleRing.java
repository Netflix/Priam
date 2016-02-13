/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.identity;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.ITokenManager;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Class providing functionality for doubling the ring
 */
public class DoubleRing
{
    private static final Logger logger = LoggerFactory.getLogger(DoubleRing.class);
    private static File TMP_BACKUP_FILE;
    private final IConfiguration config;
    private final IPriamInstanceFactory factory;
    private final ITokenManager tokenManager;

    @Inject
    public DoubleRing(IConfiguration config, IPriamInstanceFactory factory, ITokenManager tokenManager)
    {
        this.config = config;
        this.factory = factory;
        this.tokenManager = tokenManager;
    }

    /**
     * Doubling is done by pre-calculating all slots of a double ring and
     * registering them. When new nodes come up, they will get the unsed token
     * assigned per token logic.
     */
    public void doubleSlots()
    {
        List<PriamInstance> local = filteredRemote(factory.getAllIds(config.getAppName()));

        // delete all
        for (PriamInstance data : local)
            factory.delete(data);

        int hash = tokenManager.dcOffset(config.getDC());
        // move existing slots.
        for (PriamInstance data : local)
        {
            int slot = (data.getId() - hash) * 2;
            factory.create(data.getApp(), hash + slot, data.getInstanceId(), data.getHostName(), data.getHostIP(), data.getRac(), data.getVolumes(), data.getToken());
        }

        int new_ring_size = local.size() * 2;
        for (PriamInstance data : filteredRemote(factory.getAllIds(config.getAppName())))
        {
            // if max then rotate.
            int currentSlot = data.getId() - hash;
            int new_slot = currentSlot + 3 > new_ring_size ? (currentSlot + 3) - new_ring_size : currentSlot + 3;
            String token = tokenManager.createToken(new_slot, new_ring_size, config.getDC());
            factory.create(data.getApp(), new_slot + hash, InstanceIdentity.DUMMY_INSTANCE_ID, config.getHostname(), config.getHostIP(), data.getRac(), null, token);
        }
    }

    // filter other DC's
    private List<PriamInstance> filteredRemote(List<PriamInstance> lst)
    {
        List<PriamInstance> local = Lists.newArrayList();
        for (PriamInstance data : lst)
            if (data.getDC().equals(config.getDC()))
                local.add(data);
        return local;
    }

    /**
     * Backup the current state in case of failure
     */
    public void backup() throws IOException
    {
        // writing to the backup file.
        TMP_BACKUP_FILE = File.createTempFile("Backup-instance-data", ".dat");
        OutputStream out = new FileOutputStream(TMP_BACKUP_FILE);
        ObjectOutputStream stream = new ObjectOutputStream(out);
        try
        {
            stream.writeObject(filteredRemote(factory.getAllIds(config.getAppName())));
            logger.info("Wrote the backup of the instances to: " + TMP_BACKUP_FILE.getAbsolutePath());
        }
        finally
        {
            IOUtils.closeQuietly(stream);
            IOUtils.closeQuietly(out);
        }
    }

    /**
     * Restore tokens if a failure occurs
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void restore() throws IOException, ClassNotFoundException
    {
        for (PriamInstance data : filteredRemote(factory.getAllIds(config.getAppName())))
            factory.delete(data);

        // read from the file.
        InputStream in = new FileInputStream(TMP_BACKUP_FILE);
        ObjectInputStream stream = new ObjectInputStream(in);
        try
        {
            @SuppressWarnings("unchecked")
            List<PriamInstance> allInstances = (List<PriamInstance>) stream.readObject();
            for (PriamInstance data : allInstances)
                factory.create(data.getApp(), data.getId(), data.getInstanceId(), data.getHostName(), data.getHostIP(), data.getRac(), data.getVolumes(), data.getToken());
            logger.info("Sucecsfully restored the Instances from the backup: " + TMP_BACKUP_FILE.getAbsolutePath());
        }
        finally
        {
            IOUtils.closeQuietly(stream);
            IOUtils.closeQuietly(in);
        }
    }

}
