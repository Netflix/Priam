package com.priam.netflix;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.instance.identity.StorageDevice;
import com.priam.conf.IConfiguration;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.PriamInstance;

/**
 * This is a boot instance factory that will used when cassandra needs to bootup in bootstrap cluster mode.
 * 
 * @author Praveen Sadhu
 *
 */
public class NFBootInstanceFactory implements IPriamInstanceFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NFBootInstanceFactory.class);

    @Inject
    IConfiguration config;

    @Inject
    public NFBootInstanceFactory() throws FileNotFoundException
    {

    }

    @Override
    public List<PriamInstance> getAllIds(String appName)
    {
        if( appName.endsWith("-dead"))
            return Lists.newArrayList();
        return getAllInstances();
    }

    @Override
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, StorageDevice> volumes, String payload)
    {
        final List<PriamInstance> allIds = getAllInstances();
        for (PriamInstance ins : allIds)
        {
            if (ins.getInstanceId().equals(config.getInstanceName()))
                return ins;
        }
        return null;
    }

    private List<PriamInstance> getAllInstances()
    {
        List<PriamInstance> result = new ArrayList<PriamInstance>();        
        PriamInstance ins = new PriamInstance();
        ins.setApp(config.getAppName());
        ins.setRac(config.getRac());
        ins.setHost(config.getHostname());
        ins.setHostIP(config.getHostIP());
        ins.setInstanceId(config.getInstanceName());
        ins.setDC(config.getDC());
        ins.setPayload(System.getProperty("Priam.localbootstrap.token", ""));
        result.add(ins);

        String seeds = System.getProperty("Priam.localbootstrap.nodeslist", "");
        
        for (String s : seeds.split(","))
        {
            s = s.trim();
            if (!s.equals("127.0.0.1") && !s.equals(config.getHostIP()))
            {
                logger.info("Adding seed: " + s);
                ins = new PriamInstance();
                ins.setApp(config.getAppName());
                ins.setRac(config.getRac() + "_" + s.hashCode());
                ins.setHost(s);
                ins.setHostIP(s);
                ins.setInstanceId(s);
                ins.setDC(config.getDC());
                result.add(ins);
            }
        }
        return result;
    }

    @Override
    public void delete(PriamInstance inst)
    {
    }

    @Override
    public void update(PriamInstance inst)
    {
    }

    @Override
    public void sort(List<PriamInstance> return_)
    {
    }

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device)
    {
    }

}
