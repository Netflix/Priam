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
package com.netflix.priam.backupv2;

import com.google.common.collect.ImmutableSet;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This is a POJO to encapsulate all the SSTables for a given column family. Created by aagrawal on
 * 7/1/18.
 */
public class ColumnFamilyResult {
    private String keyspaceName;
    private String columnfamilyName;
    private List<SSTableResult> sstables = new ArrayList<>();

    public ColumnFamilyResult(String keyspaceName, String columnfamilyName) {
        this.keyspaceName = keyspaceName;
        this.columnfamilyName = columnfamilyName;
    }

    public List<SSTableResult> getSstables() {
        return sstables;
    }

    public void setSstables(List<SSTableResult> sstables) {
        this.sstables = sstables;
    }

    public void addSstable(SSTableResult sstable) {
        if (sstables == null) sstables = new ArrayList<>();
        sstables.add(sstable);
    }

    @Override
    public String toString() {
        return GsonJsonSerializer.getGson().toJson(this);
    }

    /** This is a POJO to encapsulate a SSTable and all its components. */
    public static class SSTableResult {
        private String prefix;
        private Set<FileUploadResult> sstableComponents;

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public Set<FileUploadResult> getSstableComponents() {
            return sstableComponents;
        }

        public void setSstableComponents(ImmutableSet<FileUploadResult> sstableComponents) {
            this.sstableComponents = sstableComponents;
        }

        @Override
        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }
    }
}
