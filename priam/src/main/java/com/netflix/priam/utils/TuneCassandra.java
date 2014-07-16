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
package com.netflix.priam.utils;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

@Singleton
public class TuneCassandra extends Task
{
	private static final Logger LOGGER = LoggerFactory.getLogger(TuneCassandra.class);
    public static final String JOBNAME = "Tune-Cassandra";
    private final CassandraTuner tuner;

    @Inject
    public TuneCassandra(IConfiguration config, CassandraTuner tuner)
    {
        super(config);
        this.tuner = tuner;
    }

    public void execute() throws IOException
    {
    	boolean isDone = false;
    	
    	while (!isDone) {
    	  try {
              tuner.writeAllProperties(config.getYamlLocation(), null, config.getSeedProviderName());
              isDone = true;
    	   } catch (IOException e) {
    		  LOGGER.info("Fail writing cassandra.yml file. Retry again!");
    	   }
    	}
    	
    }

    @Override
    public String getName()
    {
        return "Tune-Cassandra";
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }
}
