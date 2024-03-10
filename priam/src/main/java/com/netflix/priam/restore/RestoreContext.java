/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.restore;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.scheduler.PriamScheduler;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * At run-time, determine the source type to restore from.
 */
public class RestoreContext {
    private final PriamScheduler scheduler;
    private final IConfiguration config;
    private static final Logger logger = LoggerFactory.getLogger(RestoreContext.class);

    @Inject
    public RestoreContext(IConfiguration config, PriamScheduler scheduler) {
        this.config = config;
        this.scheduler = scheduler;
    }

    public boolean isRestoreEnabled() {
        return !StringUtils.isEmpty(config.getRestoreSnapshot());
    }

    public void restore() throws Exception {
        if (!isRestoreEnabled()) return;
        scheduler.addTask(
                Restore.JOBNAME,
                Restore.class,
                Restore.getTimer()); // restore from the AWS primary acct
        logger.info("Scheduled task " + Restore.JOBNAME);
    }
}
