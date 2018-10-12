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
package com.netflix.priam.backup;

/** Enum to describe the status of the snapshot/restore. */
public enum Status {
    /** Denotes snapshot/restore has started successfully and is running. */
    STARTED,
    /** Denotes snapshot/restore has finished successfully. */
    FINISHED,
    /**
     * Denotes snapshot/restore has failed to upload/restore successfully or there was a failure
     * marking the snapshot/restore as failure.
     */
    FAILED
}
