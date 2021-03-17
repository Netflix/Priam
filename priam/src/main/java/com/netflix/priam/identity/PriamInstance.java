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

import java.io.Serializable;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriamInstance implements Serializable {
    private static final long serialVersionUID = 5606412386974488659L;
    private static final Logger logger = LoggerFactory.getLogger(PriamInstance.class);
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
    // Handles Storage objects
    private Map<String, Object> volumes;

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

    public String getRac() {
        return availabilityZone;
    }

    public void setRac(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public String getHostName() {
        return hostname;
    }

    public String getHostIP() {
        return publicip;
    }

    public void setHost(String hostname, String publicip) {
        this.hostname = hostname;
        this.publicip = publicip;
    }

    public void setHost(String hostname) {
        this.hostname = hostname;
    }

    public void setHostIP(String publicip) {
        this.publicip = publicip;
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
        return String.format(
                "Hostname: %s, InstanceId: %s, APP_NAME: %s, RAC : %s, Location %s, Id: %s: Token: %s, IP: %s",
                hostname, instanceId, app, availabilityZone, location, Id, token, publicip);
    }

    public String getDC() {
        return location;
    }

    public void setDC(String location) {
        this.location = location;
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

    public PriamInstance setOutOfService() {
        this.outOfService = true;
        return this;
    }
}
