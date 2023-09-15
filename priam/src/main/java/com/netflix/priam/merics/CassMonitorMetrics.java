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

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;

/** @author vchella */
@Singleton
public class CassMonitorMetrics {
    private final Gauge cassStop, cassAutoStart, cassStart;
    private final AtomicLong getSeedsCnt, getTokenCnt, getReplacedIpCnt, doubleRingCnt;

    @Inject
    public CassMonitorMetrics(Registry registry) {
        cassStop = registry.gauge(Metrics.METRIC_PREFIX + "cass.stop");
        cassStart = registry.gauge(Metrics.METRIC_PREFIX + "cass.start");
        cassAutoStart = registry.gauge(Metrics.METRIC_PREFIX + "cass.auto.start");

        getSeedsCnt =
                PolledMeter.using(registry)
                        .withName(Metrics.METRIC_PREFIX + "cass.getSeedCnt")
                        .monitorMonotonicCounter(new AtomicLong(0));
        getTokenCnt =
                PolledMeter.using(registry)
                        .withName(Metrics.METRIC_PREFIX + "cass.getTokenCnt")
                        .monitorMonotonicCounter(new AtomicLong(0));
        getReplacedIpCnt =
                PolledMeter.using(registry)
                        .withName(Metrics.METRIC_PREFIX + "cass.getReplacedIpCnt")
                        .monitorMonotonicCounter(new AtomicLong(0));

        doubleRingCnt =
                PolledMeter.using(registry)
                        .withName(Metrics.METRIC_PREFIX + "cass.doubleRingCnt")
                        .monitorMonotonicCounter(new AtomicLong(0));
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

    public void incGetSeeds() {
        getSeedsCnt.incrementAndGet();
    }

    public void incGetToken() {
        getTokenCnt.incrementAndGet();
    }

    public void incGetReplacedIp() {
        getReplacedIpCnt.incrementAndGet();
    }

    public void incDoubleRing() {
        doubleRingCnt.incrementAndGet();
    }
}
