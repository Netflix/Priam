/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.merics;

import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupMetrics;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by vinhn on 2/13/17.
 */
@Singleton
public class BackupMetricsMgr implements IBackupMetrics{
    private AtomicInteger validUploads = new AtomicInteger()
            , invalidUploads = new AtomicInteger()
            , validDownloads = new AtomicInteger()
            , invalidDownloads = new AtomicInteger()
            ;

    @Override
    public void incrementValidUploads() {
        this.validUploads.getAndIncrement();
    }

    @Override
    public int getInvalidUploads() {
        return this.invalidUploads.get();
    }
    @Override
    public void incrementInvalidUploads() {
        this.invalidUploads.getAndIncrement();
    }

    @Override
    public int getValidUploads() {
        return this.validUploads.get();
    }

    @Override
    public int getValidDownloads() {
        return this.validDownloads.get();
    }

    @Override
    public void incrementValidDownloads() {
        this.invalidDownloads.getAndIncrement();
    }

    @Override
    public int getInvalidDownloads() {
        return this.invalidDownloads.get();
    }

    @Override
    public void incrementInvalidDownloads() {
        this.invalidDownloads.getAndIncrement();
    }

}
