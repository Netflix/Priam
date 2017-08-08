/*
 * Copyright 2016 Netflix, Inc.
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

package com.netflix.priam.dse;

import com.google.inject.ImplementedBy;

/**
 * This is intended for tuning audit log settings.
 * Audit log settings file change between cassandra version from log4j to yaml.
 * Created by aagrawal on 8/8/17.
 */
@ImplementedBy(AuditLogTunerYaml.class)
public interface IAuditLogTuner {
    void tuneAuditLog();
}
