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
package com.netflix.priam.aws;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.PriamInstance;

/**
 * SimpleDB based instance factory. Requires 'InstanceIdentity' domain to be
 * created ahead of time.
 */
@Singleton
public class SDBInstanceFactory implements IPriamInstanceFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SDBInstanceFactory.class);

    private final IConfiguration config;
    private final SDBInstanceData dao;

    @Inject
    public SDBInstanceFactory(IConfiguration config, SDBInstanceData dao)
    {
        this.config = config;
        this.dao = dao;
    }

    @Override
    public List<PriamInstance> getAllIds(String appName)
    {
        List<PriamInstance> return_ = new ArrayList<PriamInstance>();
        for (PriamInstance instance : dao.getAllIds(appName))
        {
            return_.add(instance);
        }
        sort(return_);
        return return_;
    }

    @Override
    public PriamInstance getInstance(String appName, String dc, int id)
    {
      return dao.getInstance(appName, dc, id);
    }

    @Override
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token)
    {
        try
        {
            PriamInstance ins = makePriamInstance(app, id, instanceID, hostname, ip, rac, volumes, token);
            // remove old data node which are dead.
            if (app.endsWith("-dead"))
            {
                try
                {
                    PriamInstance oldData = dao.getInstance(app, config.getDC(), id);
                    // clean up a very old data...
                    if (null != oldData && oldData.getUpdatetime() < (System.currentTimeMillis() - (3 * 60 * 1000)))
                        dao.deregisterInstance(oldData);
                }
                catch (Exception ex)
                {
                    //Do nothing
                    logger.error(ex.getMessage(), ex);
                }
            }
            dao.registerInstance(ins);
            return ins;
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(PriamInstance inst)
    {
        try
        {
            dao.deregisterInstance(inst);
        }
        catch (AmazonServiceException e)
        {
            throw new RuntimeException("Unable to deregister priam instance", e);
        }
    }

    @Override
    public void update(PriamInstance inst)
    {
        try
        {
            dao.createInstance(inst);
        }
        catch (AmazonServiceException e)
        {
            throw new RuntimeException("Unable to update/create priam instance", e);
        }
    }

    @Override
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

    @Override
    public void attachVolumes(PriamInstance instance, String mountPath, String device)
    {
        // TODO Auto-generated method stub
    }

    private PriamInstance makePriamInstance(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token)
    {
        Map<String, Object> v = (volumes == null) ? new HashMap<String, Object>() : volumes;
        PriamInstance ins = new PriamInstance();
        ins.setApp(app);
        ins.setRac(rac);
        ins.setHost(hostname);
        ins.setHostIP(ip);
        ins.setId(id);
        ins.setInstanceId(instanceID);
        ins.setDC(config.getDC());
        ins.setToken(token);
        ins.setVolumes(v);
        return ins;
    }
}
