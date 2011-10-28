package com.priam.netflix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.HintedHandOffManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.instance.identity.IdentityPersistenceException;
import com.netflix.instance.identity.InstanceData;
import com.netflix.instance.identity.StorageDevice;
import com.netflix.instance.identity.aws.InstanceDataDAOSimpleDb;
import com.netflix.tools.aws.EC2Utils;
import com.priam.conf.IConfiguration;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.PriamInstance;
import com.priam.utils.SystemUtils;

@Singleton
public class NFInstanceFactory implements IPriamInstanceFactory
{
    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);
    private static final int MOUNT_SIZE = 5;
    private static final String MOUNT_TYPE = "xfs";
    private EC2Utils utils;

    @Inject
    IConfiguration config;

    @Inject
    public NFInstanceFactory() {
        this.utils = new EC2Utils();
    }

    public List<PriamInstance> getAllIds(String appName)
    {
        List<PriamInstance> return_ = new ArrayList<PriamInstance>();
        try {
            for (InstanceData data : InstanceDataDAOSimpleDb.getInstance().getAllIds(appName)) {
                return_.add(transform(data));
            }
            sort(return_);
            return return_;
        }
        catch (IdentityPersistenceException e) {
            throw new RuntimeException(e);
        }
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
        try {
            Map<String, StorageDevice> v = volumes == null ? new HashMap<String, StorageDevice>() : volumes;
            InstanceData nfins = new InstanceData(app, id, instanceID, zone, 0, v, hostname + "," + ip, new Date(System.currentTimeMillis()));
            nfins.setPayload(payload);
            nfins.setLocation(config.getDC());
            // remove old data node which are dead.
            if (app.endsWith("-dead")) {
                try {
                    InstanceData oldData = InstanceDataDAOSimpleDb.getInstance().getInstance(app, id);
                    // clean up a very old data...
                    if (null != oldData) {
                        // delete after 3 min.
                        if (oldData.getUpdateTimestamp().getTime() < (System.currentTimeMillis() + (3 * 60 * 1000)))
                            InstanceDataDAOSimpleDb.getInstance().deregisterInstance(oldData);
                    }
                }
                catch (Exception ex) {
                    // dont do anything.
                }
            }
            InstanceDataDAOSimpleDb.getInstance().registerInstance(nfins);
            return transform(nfins);
        }
        catch (IdentityPersistenceException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(PriamInstance inst)
    {
        try {
            InstanceDataDAOSimpleDb.getInstance().deleteInstanceEntry(transform(inst));
        }
        catch (IdentityPersistenceException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(PriamInstance inst)
    {
        try {
            InstanceDataDAOSimpleDb.getInstance().createInstanceEntry(transform(inst));
        }
        catch (IdentityPersistenceException e) {
            throw new RuntimeException(e);
        }
    }

    private PriamInstance transform(InstanceData data)
    {
        PriamInstance ins = new PriamInstance();
        ins.setApp(data.getApp());
        ins.setRac(data.getAvailabilityZone());
        String[] split = data.getElasticIP().split(",");
        ins.setHost(split[0], split[1]);
        ins.setId(data.getId());
        ins.setInstanceId(data.getInstanceId());
        ins.setDC(data.getLocation());
        ins.setPayload(data.getPayload());
        ins.setVolumes(data.getVolumes());
        return ins;
    }

    private InstanceData transform(PriamInstance data)
    {
        InstanceData ins = new InstanceData(data.getApp(), data.getId());
        ins.setAvailabilityZone(data.getRac());
        ins.setElasticIP(data.getHostName() + "," + data.getHostIP());
        ins.setInstanceId(data.getInstanceId());
        ins.setLocation(data.getDC());
        ins.setPayload(data.getPayload());
        ins.setVolumes(data.getVolumes());
        return ins;
    }

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device)
    {
        try {
            Map<String, StorageDevice> vols = null == instance.getVolumes() ? new HashMap<String, StorageDevice>() : instance.getVolumes();
            if (null == instance.getVolumes().get(mountPath)) {
                StorageDevice dev = new StorageDevice(utils.createEBSVolume(config.getAppName(), MOUNT_SIZE), device, mountPath, MOUNT_TYPE, null, null, MOUNT_SIZE);
                vols.put(dev.getMountPoint(), dev);
                instance.setVolumes(vols);
            }
            // attach the created volumes.
            utils.attachVolume(instance.getVolumes().get(mountPath).getId(), device, config.getInstanceName());
        }
        catch (Throwable th) {
            logger.error("Error while attaching....", th);
        }
        logger.info(String.format("Attaching volume: %s to the host %s", instance.getVolumes().get(mountPath).toString(), instance.getVolumes().get(mountPath).getId()));
        try {
            logger.info("Sleeping for 60 Sec while volumes are attached.");
            Thread.sleep(60000); // 60 Sec's
            mountAll(instance.volumes);
        }
        catch (InterruptedException ex) {
            logger.warn("Interepted: ", ex);
        }
        catch (IOException e) {
            logger.error("Error while mounting.", e);
        }
    }

    /**
     * Mounts all the file systems in the volumes map.
     */
    public void mountAll(Map<String, StorageDevice> volumes) throws IOException, InterruptedException
    {
        for (Entry<String, StorageDevice> entry : volumes.entrySet())
            SystemUtils.mount(entry.getValue().getDevice(), entry.getValue().getMountPoint());
    }
}
