package com.priam.netflix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.instance.identity.StorageDevice;
import com.netflix.tools.aws.EC2Utils;
import com.priam.conf.IConfiguration;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.PriamInstance;
import com.priam.utils.SystemUtils;

/**
 * Factory to use cassandra for managing instance data
 * 
 * @author Praveen Sadhu
 */
@Singleton
public class CassandraInstanceFactory implements IPriamInstanceFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraInstanceFactory.class);
    private static final int MOUNT_SIZE = 5;
    private static final String MOUNT_TYPE = "xfs";
    private EC2Utils utils;

    @Inject
    IConfiguration config;

    @Inject
    InstanceDataDAOCassandra instanceDataDAO;

    @Inject
    public CassandraInstanceFactory()
    {
        this.utils = new EC2Utils();
    }

    public List<PriamInstance> getAllIds(String appName)
    {
        List<PriamInstance> return_ = new ArrayList<PriamInstance>();
        for (PriamInstance instance : instanceDataDAO.getAllInstances(appName))
        {
            return_.add(instance);
        }

        sort(return_);
        return return_;
    }

    public void sort(List<PriamInstance> return_)
    {
        Comparator<? super PriamInstance> comparator = new Comparator<PriamInstance>()
        {

            @Override
            public int compare(PriamInstance o1, PriamInstance o2)
            {
                Integer c1 = o1.getId();
                Integer c2 = o2.getId();
                return c1.compareTo(c2);
            }
        };
        Collections.sort(return_, comparator);
    }

    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String zone, Map<String, StorageDevice> volumes, String payload)
    {
        try
        {
            Map<String, StorageDevice> v = volumes == null ? new HashMap<String, StorageDevice>() : volumes;
            PriamInstance ins = new PriamInstance();
            ins.setApp(app);
            ins.setRac(zone);
            ins.setHost(hostname);
            ins.setHostIP(ip);
            ins.setId(id);
            ins.setInstanceId(instanceID);
            ins.setDC(config.getDC());
            ins.setPayload(payload);
            ins.setVolumes(volumes);

            // remove old data node which are dead.
            if (app.endsWith("-dead"))
            {
                try
                {
                    PriamInstance oldData = instanceDataDAO.getInstance(app, id);
                    // clean up a very old data...
                    if (null != oldData)
                        instanceDataDAO.deleteInstanceEntry(oldData);

                }
                catch (Exception ex)
                {
                    logger.error(ex.getMessage(), ex);
                }
            }
            instanceDataDAO.createInstanceEntry(ins);
            return ins;
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void delete(PriamInstance inst)
    {
        try
        {
            instanceDataDAO.deleteInstanceEntry(inst);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void update(PriamInstance inst)
    {
        try
        {
            instanceDataDAO.createInstanceEntry(inst);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device)
    {
        try
        {
            Map<String, StorageDevice> vols = null == instance.getVolumes() ? new HashMap<String, StorageDevice>() : instance.getVolumes();
            if (null == instance.getVolumes().get(mountPath))
            {
                StorageDevice dev = new StorageDevice(utils.createEBSVolume(config.getAppName(), MOUNT_SIZE), device, mountPath, MOUNT_TYPE, null, null, MOUNT_SIZE);
                vols.put(dev.getMountPoint(), dev);
                instance.setVolumes(vols);
            }
            // attach the created volumes.
            utils.attachVolume(instance.getVolumes().get(mountPath).getId(), device, config.getInstanceName());
        }
        catch (Throwable th)
        {
            logger.error("Error while attaching volume", th);
        }
        logger.info(String.format("Attaching volume: %s to the host %s", instance.getVolumes().get(mountPath).toString(), instance.getVolumes().get(mountPath).getId()));
        try
        {
            logger.info("Sleeping for 60 Sec while volumes are attached.");
            Thread.sleep(60000); // 60 Sec's
            mountAll(instance.volumes);
        }
        catch (InterruptedException ex)
        {
            logger.warn("Interepted: ", ex);
        }
        catch (IOException e) {
            logger.error("Error while mounting.", e);
        }
    }
    
    public void mountAll(Map<String, StorageDevice> volumes) throws IOException, InterruptedException
    {
        for (Entry<String, StorageDevice> entry : volumes.entrySet())
            SystemUtils.mount(entry.getValue().getDevice(), entry.getValue().getMountPoint());
    }
}
