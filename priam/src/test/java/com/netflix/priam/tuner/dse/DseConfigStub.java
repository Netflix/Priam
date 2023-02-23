/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.tuner.dse;

import com.netflix.priam.config.FakeConfiguration;
import java.util.HashSet;
import java.util.Set;

public class DseConfigStub implements IDseConfiguration {
    boolean auditLogEnabled;

    public String getDseYamlLocation() {
        return new FakeConfiguration().getCassHome() + "/resources/dse/conf/dse.yaml";
    }

    public String getDseDelegatingSnitch() {
        return null;
    }

    public NodeType getNodeType() {
        return null;
    }

    public boolean isAuditLogEnabled() {
        return auditLogEnabled;
    }

    public void setAuditLogEnabled(boolean b) {
        auditLogEnabled = b;
    }

    public String getAuditLogExemptKeyspaces() {
        return "YourSwellKeyspace";
    }

    public Set<AuditLogCategory> getAuditLogCategories() {
        return new HashSet<AuditLogCategory>() {
            {
                this.add(AuditLogCategory.ALL);
            }
        };
    }
}
