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

import com.google.inject.ImplementedBy;

/**
 * A means to keep track of events with Cassandra Monitor (Start, restart, stop etc.,)
 * Created by vchella on 01/10/18.
 */
@ImplementedBy(CassMonitorMetrics.class)
public interface ICassMonitorMetrics {
    void incCassStop();
    void incCassAutoStart();
    void incCassStart();

    double getCassStop();
    double getCassAutoStart();
    double getCassStart();
}
