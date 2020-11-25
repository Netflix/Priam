/*
 * Copyright 2018 Netflix, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.priam.identity.config.InstanceInfo;
import java.util.*;

public class FakePriamInstanceFactory implements IPriamInstanceFactory<PriamInstance> {
    private final Map<Integer, PriamInstance> instances = Maps.newHashMap();
    private final InstanceInfo instanceInfo;

    @Inject
    public FakePriamInstanceFactory(InstanceInfo instanceInfo) {
        this.instanceInfo = instanceInfo;
    }

    @Override
    public List<PriamInstance> getAllIds(String appName) {
        if (appName.endsWith("-dead")) return ImmutableList.of();
        List<PriamInstance> result = new ArrayList<>(instances.values());
        sort(result);
        return result;
    }

    @Override
    public PriamInstance getInstance(String appName, String dc, int id) {
        return instances.get(id);
    }

    @Override
    public PriamInstance create(
            String app,
            int id,
            String instanceID,
            String hostname,
            String ip,
            String rac,
            Map<String, Object> volumes,
            String payload) {
        PriamInstance ins = new PriamInstance();
        ins.setApp(app);
        ins.setRac(rac);
        ins.setHost(hostname, ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setToken(payload);
        ins.setVolumes(volumes);
        ins.setDC(instanceInfo.getRegion());
        instances.put(id, ins);
        return ins;
    }

    @Override
    public void delete(PriamInstance inst) {
        instances.remove(inst.getId());
    }

    @Override
    public void update(PriamInstance orig, PriamInstance inst) {
        instances.put(inst.getId(), inst);
    }

    @Override
    public void sort(List<PriamInstance> return_) {
        Comparator<? super PriamInstance> comparator =
                (Comparator<PriamInstance>)
                        (o1, o2) -> {
                            Integer c1 = o1.getId();
                            Integer c2 = o2.getId();
                            return c1.compareTo(c2);
                        };
        return_.sort(comparator);
    }

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device) {
        // TODO Auto-generated method stub
    }
}
