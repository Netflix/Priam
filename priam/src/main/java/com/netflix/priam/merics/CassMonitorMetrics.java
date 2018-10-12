/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.merics;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;

/** @author vchella */
@Singleton
public class CassMonitorMetrics {
    private final Gauge cassStop, cassAutoStart, cassStart;

    @Inject
    public CassMonitorMetrics(Registry registry) {
        cassStop = registry.gauge(Metrics.METRIC_PREFIX + "cass.stop");
        cassStart = registry.gauge(Metrics.METRIC_PREFIX + "cass.start");
        cassAutoStart = registry.gauge(Metrics.METRIC_PREFIX + "cass.auto.start");
    }

    public void incCassStop() {
        cassStop.set(cassStop.value() + 1);
    }

    public void incCassAutoStart() {
        cassAutoStart.set(cassAutoStart.value() + 1);
    }

    public void incCassStart() {
        cassStart.set(cassStart.value() + 1);
    }
}
