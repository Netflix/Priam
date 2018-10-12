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

import com.netflix.priam.utils.GsonJsonSerializer;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a POJO to encapsulate all the SSTables for a given column family. Created by aagrawal on
 * 7/1/18.
 */
public class ColumnfamilyResult {
    private String keyspaceName;
    private String columnfamilyName;
    private List<SSTableResult> sstables = new ArrayList<>();

    public ColumnfamilyResult(String keyspaceName, String columnfamilyName) {
        this.keyspaceName = keyspaceName;
        this.columnfamilyName = columnfamilyName;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getColumnfamilyName() {
        return columnfamilyName;
    }

    public void setColumnfamilyName(String columnfamilyName) {
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
        private List<FileUploadResult> sstableComponents;

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public List<FileUploadResult> getSstableComponents() {
            return sstableComponents;
        }

        public void setSstableComponents(List<FileUploadResult> sstableComponents) {
            this.sstableComponents = sstableComponents;
        }

        @Override
        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }
    }
}
