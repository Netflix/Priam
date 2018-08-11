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

import com.google.inject.Singleton;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author vchella
 */
@Singleton
public class CassMonitorMetrics implements  ICassMonitorMetrics {

    private AtomicInteger cassStop = new AtomicInteger();
    private AtomicInteger cassAutoStart = new AtomicInteger();
    private AtomicInteger cassStart = new AtomicInteger();


    @Override
    public void incCassStop() {
        cassStop.getAndIncrement();
    }

    @Override
    public void incCassAutoStart() {
        cassAutoStart.getAndIncrement();
    }

    @Override
    public void incCassStart() {
        cassStart.getAndIncrement();
    }

    @Override
    public int getCassStop() {
        return cassStop.get();
    }

    @Override
    public int getCassAutoStart() {
        return cassAutoStart.get();
    }

    @Override
    public int getCassStart() {
        return cassStart.get();
    }
}
