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
package com.netflix.priam.backupv2;

import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.GsonJsonSerializer;
import java.time.Instant;
import java.util.List;

/** This POJO class encapsulates the information for a meta file. */
public class MetaFileInfo {
    @GsonJsonSerializer.PriamAnnotation.GsonIgnore
    public static final String META_FILE_PREFIX = "meta_v2_";

    @GsonJsonSerializer.PriamAnnotation.GsonIgnore
    public static final String META_FILE_SUFFIX = ".json";

    @GsonJsonSerializer.PriamAnnotation.GsonIgnore
    public static final String META_FILE_INFO = "info";

    @GsonJsonSerializer.PriamAnnotation.GsonIgnore
    public static final String META_FILE_DATA = "data";

    private short version = 1;
    private String appName;
    private String region;
    private String rack;

    public void setVersion(short version) {
        this.version = version;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public void setBackupIdentifier(List<String> backupIdentifier) {
        this.backupIdentifier = backupIdentifier;
    }

    private List<String> backupIdentifier;

    public MetaFileInfo(String appName, String region, String rack, List<String> backupIdentifier) {
        this.appName = appName;
        this.region = region;
        this.rack = rack;
        this.backupIdentifier = backupIdentifier;
    }

    public short getVersion() {
        return version;
    }

    public String getAppName() {
        return appName;
    }

    public String getRegion() {
        return region;
    }

    public String getRack() {
        return rack;
    }

    public List<String> getBackupIdentifier() {
        return backupIdentifier;
    }

    @Override
    public String toString() {
        return GsonJsonSerializer.getGson().toJson(this);
    }

    public static String getMetaFileName(Instant instant) {
        return MetaFileInfo.META_FILE_PREFIX
                + DateUtil.formatInstant(DateUtil.yyyyMMddHHmm, instant)
                + MetaFileInfo.META_FILE_SUFFIX;
    }
}
