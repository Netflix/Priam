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
package com.netflix.priam.merics;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Measurement class for scheduled compactions Created by aagrawal on 2/28/18. */
@Singleton
public class CompactionMeasurement implements IMeasurement {
    private final Counter failure, success;

    @Inject
    public CompactionMeasurement(Registry registry) {
        failure = registry.counter(Metrics.METRIC_PREFIX + "compaction.failure");
        success = registry.counter(Metrics.METRIC_PREFIX + "compaction.success");
    }

    public void incrementFailure() {
        this.failure.increment();
    }

    public void incrementSuccess() {
        success.increment();
    }
}
