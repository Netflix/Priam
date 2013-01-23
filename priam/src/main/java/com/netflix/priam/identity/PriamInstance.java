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

import java.io.Serializable;
import java.util.Map;

import com.netflix.priam.IConfiguration;

public class PriamInstance implements Serializable
{
    private static final long serialVersionUID = 5606412386974488659L;
    private String hostname;
    private boolean outOfService;

    private String instanceId;
    private String rack;
    private String publicip;
    private String datacenter;

    public PriamInstance(String hostname, String instanceId, String datacenter, String rack, String publicip)
    {
        this.hostname = hostname;
        this.instanceId = instanceId;
        this.datacenter = datacenter;
        this.rack = rack;
        this.publicip = publicip;
    }

    public PriamInstance(IConfiguration config)
    {
        this.hostname = config.getHostname();
        this.instanceId = config.getInstanceName();
        this.datacenter = config.getDC();
        this.rack = config.getRac();
        this.publicip = config.getHostIP();
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public String getRac()
    {
        return rack;
    }

    public String getHostName()
    {
        return hostname;
    }
    
    public String getHostIP()
    {
        return publicip;
    }

    public String toString()
    {
        return String.format("Hostname: %s, InstanceId: %s, APP_NAME: %s, Rack : %s Datacenter %s", getHostName(), getInstanceId(), getRac(), getDC());
    }

    public String getDC()
    {
        return datacenter;
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
