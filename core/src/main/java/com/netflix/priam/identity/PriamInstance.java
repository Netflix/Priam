package com.netflix.priam.identity;

import org.apache.commons.lang.builder.CompareToBuilder;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class PriamInstance implements Serializable, Comparable<PriamInstance> {
    private static final long serialVersionUID = 5606412386974488659L;
    private String hostname;
    private long updatetime;
    private boolean outOfService;

    private String app;
    private int Id;
    private String instanceId;
    private String availabilityZone;
    private String hostIp;
    private String regionName;
    private String token;
    //Handles Storage objects
    private Map<String, Object> volumes;

    public static PriamInstance from(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token, String regionName) {
        Map<String, Object> v = (volumes == null) ? new HashMap<String, Object>() : volumes;
        PriamInstance ins = new PriamInstance();
        ins.setApp(app);
        ins.setAvailabilityZone(rac);
        ins.setHost(hostname);
        ins.setHostIP(ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setRegionName(regionName);
        ins.setToken(token);
        ins.setVolumes(v);
        return ins;
    }


    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public int getId() {
        return Id;
    }

    public void setId(int id) {
        Id = id;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public String getHostName() {
        return hostname;
    }

    public String getHostIP() {
        return hostIp;
    }

    public void setHost(String hostname, String hostIp) {
        this.hostname = hostname;
        this.hostIp = hostIp;
    }

    public void setHost(String hostname) {
        this.hostname = hostname;
    }

    public void setHostIP(String hostIp) {
        this.hostIp = hostIp;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Map<String, Object> getVolumes() {
        return volumes;
    }

    public void setVolumes(Map<String, Object> volumes) {
        this.volumes = volumes;
    }

    @Override
    public String toString() {
        return String.format("Hostname: %s, InstanceId: %s, APP_NAME: %s, RAC: %s, Location: %s, Id: %s, Token: %s, Updated: %s",
                getHostName(), getInstanceId(), getApp(), getAvailabilityZone(), getRegionName(), getId(), getToken(),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(getUpdatetime()));
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public long getUpdatetime() {
        return updatetime;
    }

    public void setUpdatetime(long updatetime) {
        this.updatetime = updatetime;
    }

    public boolean isOutOfService() {
        return outOfService;
    }

    public void setOutOfService(boolean outOfService) {
        this.outOfService = outOfService;
    }

    @Override
    public int compareTo(PriamInstance o) {
        return new CompareToBuilder()
                .append(getRegionName(), o.getRegionName())
                .append(getAvailabilityZone(), o.getAvailabilityZone())
                .append(getId(), o.getId())
                .append(getInstanceId(), o.getInstanceId())
                .toComparison();
    }
}
