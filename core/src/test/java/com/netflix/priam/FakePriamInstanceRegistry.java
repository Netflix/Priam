package com.netflix.priam;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.identity.PriamInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FakePriamInstanceRegistry implements IPriamInstanceRegistry
{
    private final Map<Integer,PriamInstance> instances = Maps.newHashMap();
    private final AmazonConfiguration config;

    @Inject
    public FakePriamInstanceRegistry(AmazonConfiguration config)
    {
        this.config = config;
    }

    @Override
    public List<PriamInstance> getAllIds(String appName)
    {
        return new ArrayList<PriamInstance>(instances.values());
    }
    
    @Override
    public PriamInstance getInstance(String appName, int id) {
      return instances.get(id);
    }

    @Override
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String payload)
    {
        PriamInstance ins = new PriamInstance();
        ins.setApp(app);
        ins.setAvailabilityZone(rac);
        ins.setHost(hostname, ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setToken(payload);
        ins.setVolumes(volumes);
        ins.setRegionName(config.getRegionName());
        instances.put(id, ins);
        return ins;
    }

    @Override
    public void delete(PriamInstance inst)
    {
        instances.remove(inst.getId());
    }

    @Override
    public void update(PriamInstance inst)
    {
        instances.put(inst.getId(), inst);
    }

    @Override
    public void sort(List<PriamInstance> return_)
    {
        Collections.sort(return_);
    }

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device)
    {
        // TODO Auto-generated method stub
    }
}
