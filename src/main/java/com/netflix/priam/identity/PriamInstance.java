package com.netflix.priam.identity;

import java.io.Serializable;
import java.util.Map;

public class PriamInstance implements Serializable
{
    private static final long serialVersionUID = 5606412386974488659L;
    private String hostname;
    private long updatetime;
    private boolean outOfService;

    private String app;
    private int Id;
    private String instanceId;
    private String availabilityZone;
    private String publicip;
    private String location;
    private String token;
    //Handles Storage objects
    private Map<String, Object> volumes;
    
    public String getApp()
    {
        return app;
    }

    public void setApp(String app)
    {
        this.app = app;
    }

    public int getId()
    {
        return Id;
    }

    public void setId(int id)
    {
        Id = id;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public void setInstanceId(String instanceId)
    {
        this.instanceId = instanceId;
    }

    public String getRac()
    {
        return availabilityZone;
    }

    public void setRac(String availabilityZone)
    {
        this.availabilityZone = availabilityZone;
    }

    public String getHostName()
    {
        return hostname;
    }
    
    public String getHostIP()
    {
        return publicip;
    }

    public void setHost(String hostname, String publicip)
    {
        this.hostname = hostname;
        this.publicip = publicip;
    }

    public void setHost(String hostname)
    {
        this.hostname = hostname;
    }

    public void setHostIP(String publicip)
    {
        this.publicip = publicip;
    }

    public String getToken()
    {
        return token;
    }

    public void setToken(String token)
    {
        this.token = token;
    }

    public Map<String, Object> getVolumes()
    {
        return volumes;
    }

    public void setVolumes(Map<String, Object> volumes)
    {
        this.volumes = volumes;
    }

    @Override
    public String toString()
    {
        return String.format("Hostname: %s, InstanceId: %s, APP_NAME: %s, RAC : %s Location %s, Id: %s: Token: %s", getHostName(), getInstanceId(), getApp(), getRac(), getDC(), getId(),
                getToken());
    }

    public String getDC()
    {
        return location;
    }
    
    public void setDC(String location)
    {
        this.location = location;
    }

    public long getUpdatetime()
    {
        return updatetime;
    }

    public void setUpdatetime(long updatetime)
    {
        this.updatetime = updatetime;
    }

    public boolean isOutOfService()
    {
        return outOfService;
    }

    public void setOutOfService(boolean outOfService)
    {
        this.outOfService = outOfService;
    }


}
