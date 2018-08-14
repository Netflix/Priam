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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;

/**
 * Created by aagrawal on 8/13/18.
 */
@Singleton
public class NotificationMeasurement implements IMeasurement {
    private final Counter failure, success;

    @Inject
    public NotificationMeasurement(Registry registry) {
        failure = registry.counter("priam.aws.sns.failure");
        success = registry.counter("priam.aws.sns.success");
    }

    public void incrementFailureCnt(long val) {
        this.failure.increment();
    }

    public long getFailureCnt() {
        return this.failure.count();
    }

    public void incrementSuccessCnt(long val) {
        this.success.increment();
    }

    public long getSuccessCnt() {
        return this.success.count();
    }
}
