package com.priam.identity;

import java.util.List;
import java.util.Map;

public interface IPriamInstanceFactory
{
    public List<PriamInstance> getAllIds(String appName);

    /**
     * This will create a instance of the Server Instance with its info...
     */
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String payload);

    public void delete(PriamInstance inst);

    public void update(PriamInstance inst);

    public void sort(List<PriamInstance> return_);

    public void attachVolumes(PriamInstance instance, String mountPath, String device);
}