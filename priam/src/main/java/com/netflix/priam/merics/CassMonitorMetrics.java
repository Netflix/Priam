/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.merics;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author vchella
 */
@Singleton
public class CassMonitorMetrics implements  ICassMonitorMetrics {

    private final Gauge cassStop, cassAutoStart, cassStart;
    private final AtomicInteger i_cassStop = new AtomicInteger(0);
    private final AtomicInteger i_cassAutoStart = new AtomicInteger(0);
    private final AtomicInteger i_cassStart = new AtomicInteger(0);

    @Inject
    public CassMonitorMetrics(Registry registry){
        cassStop = registry.gauge("priam.cass.stop");
        cassStart = registry.gauge("priam.cass.start");
        cassAutoStart = registry.gauge("priam.cass.auto.start");
    }

    @Override
    public void incCassStop() {
        cassStop.set(i_cassStop.incrementAndGet());
    }

    @Override
    public void incCassAutoStart() {
        cassAutoStart.set(i_cassAutoStart.incrementAndGet());
    }

    @Override
    public void incCassStart() {
        cassStart.set(i_cassStart.incrementAndGet());
    }

    @Override
    public double getCassStop() {
        return cassStop.value();
    }

    @Override
    public double getCassAutoStart() {
        return cassAutoStart.value();
    }

    @Override
    public double getCassStart() {
        return cassStart.value();
    }
}
