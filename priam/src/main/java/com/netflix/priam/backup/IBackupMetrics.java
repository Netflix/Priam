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
package com.netflix.priam.backup;

/**
 * A means to keep track of various metata about backups
 * Created by vinhn on 2/13/17.
 */
public interface IBackupMetrics {
    public int getValidUploads();
    public void incrementValidUploads();
    public int getInvalidUploads();  //defers the semantic of "invalid upload" to implementation
    public void incrementInvalidUploads();
    public int getValidDownloads();
    public void incrementValidDownloads();
    public int getInvalidDownloads();
    public void incrementInvalidDownloads();
}
