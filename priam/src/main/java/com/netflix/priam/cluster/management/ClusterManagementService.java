package com.netflix.priam.cluster.management;
/*
 * Copyright 2019 Netflix, Inc.
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

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.scheduler.PriamScheduler;
import javax.inject.Inject;

public class ClusterManagementService implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration config;

    @Inject
    public ClusterManagementService(IConfiguration configuration, PriamScheduler priamScheduler) {
        this.scheduler = priamScheduler;
        this.config = configuration;
    }

    @Override
    public void scheduleService() throws Exception {
        // Set up nodetool flush task
        scheduleTask(scheduler, Flush.class, Flush.getTimer(config));

        // Set up compaction task
        scheduleTask(scheduler, Compaction.class, Compaction.getTimer(config));
    }

    @Override
    public void updateServicePre() throws Exception {}

    @Override
    public void updateServicePost() throws Exception {}
}
