package com.netflix.priam.identity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;

import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.utils.TokenManager;

/**
 * Class providing functionality for doubling the ring
 */
public class DoubleRing
{
    private static final Logger logger = LoggerFactory.getLogger(DoubleRing.class);
    private static File TMP_BACKUP_FILE;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final IPriamInstanceFactory factory;

    @Inject
    public DoubleRing(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, IPriamInstanceFactory factory)
    {
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.factory = factory;
    }

    /**
     * Doubling is done by pre-calculating all slots of a double ring and
     * registering them. When new nodes come up, they will get the unsed token
     * assigned per token logic.
     */
    public void doubleSlots()
    {
        List<PriamInstance> local = filteredRemote(factory.getAllIds(cassandraConfiguration.getClusterName()));

        // delete all
        for (PriamInstance data : local)
            factory.delete(data);

        int hash = TokenManager.regionOffset(amazonConfiguration.getRegionName());
        // move existing slots.
        for (PriamInstance data : local)
        {
            int slot = (data.getId() - hash) * 2;
            factory.create(data.getApp(), hash + slot, data.getInstanceId(), data.getHostName(), data.getHostIP(), data.getAvailabilityZone(), data.getVolumes(), data.getToken());
        }

        int new_ring_size = local.size() * 2;
        for (PriamInstance data : filteredRemote(factory.getAllIds(cassandraConfiguration.getClusterName())))
        {
            // if max then rotate.
            int currentSlot = data.getId() - hash;
            int new_slot = currentSlot + 3 > new_ring_size ? (currentSlot + 3) - new_ring_size : currentSlot + 3;
            String token = TokenManager.createToken(new_slot, new_ring_size, amazonConfiguration.getRegionName());
            factory.create(data.getApp(), new_slot + hash, "new_slot", amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), data.getAvailabilityZone(), null, token);
        }
    }

    // filter other DC's
    private List<PriamInstance> filteredRemote(List<PriamInstance> lst)
    {
        List<PriamInstance> local = Lists.newArrayList();
        for (PriamInstance data : lst)
            if (data.getDC().equals(amazonConfiguration.getRegionName()))
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
            stream.writeObject(filteredRemote(factory.getAllIds(cassandraConfiguration.getClusterName())));
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
        for (PriamInstance data : filteredRemote(factory.getAllIds(cassandraConfiguration.getClusterName())))
            factory.delete(data);

        // read from the file.
        InputStream in = new FileInputStream(TMP_BACKUP_FILE);
        ObjectInputStream stream = new ObjectInputStream(in);
        try
        {
            @SuppressWarnings("unchecked")
            List<PriamInstance> allInstances = (List<PriamInstance>) stream.readObject();
            for (PriamInstance data : allInstances)
                factory.create(data.getApp(), data.getId(), data.getInstanceId(), data.getHostName(), data.getHostIP(), data.getAvailabilityZone(), data.getVolumes(), data.getToken());
            logger.info("Sucecsfully restored the Instances from the backup: " + TMP_BACKUP_FILE.getAbsolutePath());
        }
        finally
        {
            IOUtils.closeQuietly(stream);
            IOUtils.closeQuietly(in);
        }
    }

}
