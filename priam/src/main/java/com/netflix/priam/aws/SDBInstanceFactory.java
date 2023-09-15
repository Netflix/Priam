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
package com.netflix.priam.aws;

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.ImmutableSet;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SimpleDB based instance instanceIdentity. Requires 'InstanceIdentity' domain to be created ahead
 */
@Singleton
public class SDBInstanceFactory implements IPriamInstanceFactory {
    private static final Logger logger = LoggerFactory.getLogger(SDBInstanceFactory.class);

    private final SDBInstanceData dao;
    private final InstanceInfo instanceInfo;

    @Inject
    public SDBInstanceFactory(SDBInstanceData dao, InstanceInfo instanceInfo) {
        this.dao = dao;
        this.instanceInfo = instanceInfo;
    }

    @Override
    public ImmutableSet<PriamInstance> getAllIds(String appName) {
        return ImmutableSet.copyOf(
                dao.getAllIds(appName)
                        .stream()
                        .sorted((Comparator.comparingInt(PriamInstance::getId)))
                        .collect(Collectors.toList()));
    }

    @Override
    public PriamInstance getInstance(String appName, String dc, int id) {
        return dao.getInstance(appName, dc, id);
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
            String token) {
        try {
            PriamInstance ins =
                    makePriamInstance(app, id, instanceID, hostname, ip, rac, volumes, token);
            // remove old data node which are dead.
            if (app.endsWith("-dead")) {
                try {
                    PriamInstance oldData = dao.getInstance(app, instanceInfo.getRegion(), id);
                    // clean up a very old data...
                    if (null != oldData
                            && oldData.getUpdatetime()
                                    < (System.currentTimeMillis() - (3 * 60 * 1000)))
                        dao.deregisterInstance(oldData);
                } catch (Exception ex) {
                    // Do nothing
                    logger.error(ex.getMessage(), ex);
                }
            }
            dao.registerInstance(ins);
            return ins;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(PriamInstance inst) {
        try {
            dao.deregisterInstance(inst);
        } catch (AmazonServiceException e) {
            throw new RuntimeException("Unable to deregister priam instance", e);
        }
    }

    @Override
    public void update(PriamInstance orig, PriamInstance inst) {
        try {
            dao.updateInstance(orig, inst);
        } catch (AmazonServiceException e) {
            throw new RuntimeException("Unable to update/create priam instance", e);
        }
    }

    private PriamInstance makePriamInstance(
            String app,
            int id,
            String instanceID,
            String hostname,
            String ip,
            String rac,
            Map<String, Object> volumes,
            String token) {
        Map<String, Object> v = (volumes == null) ? new HashMap<>() : volumes;
        PriamInstance ins = new PriamInstance();
        ins.setApp(app);
        ins.setRac(rac);
        ins.setHost(hostname);
        ins.setHostIP(ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setDC(instanceInfo.getRegion());
        ins.setToken(token);
        ins.setVolumes(v);
        return ins;
    }
}
