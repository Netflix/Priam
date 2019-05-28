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

package com.netflix.priam.tuner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Singleton;

/**
 * List of Garbage collection parameters for CMS/G1GC. This list is used to automatically
 * enable/disable configurations, if found in jvm.options. Created by aagrawal on 8/23/17.
 */
@Singleton
public class GCTuner {
    private static final Set<String> cmsOptions =
            new HashSet<>(
                    Arrays.asList(
                            "-XX:+UseConcMarkSweepGC",
                            "-XX:+UseParNewGC",
                            "-XX:+UseParallelGC",
                            "-XX:+CMSConcurrentMTEnabled",
                            "-XX:CMSInitiatingOccupancyFraction",
                            "-XX:+UseCMSInitiatingOccupancyOnly",
                            "-XX:+CMSClassUnloadingEnabled",
                            "-XX:+CMSIncrementalMode",
                            "-XX:+CMSPermGenSweepingEnabled",
                            "-XX:+ExplicitGCInvokesConcurrent",
                            "-XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses",
                            "-XX:+DisableExplicitGC",
                            "-XX:+CMSParallelRemarkEnabled",
                            "-XX:SurvivorRatio",
                            "-XX:MaxTenuringThreshold",
                            "-XX:CMSWaitDuration",
                            "-XX:+CMSParallelInitialMarkEnabled",
                            "-XX:+CMSEdenChunksRecordAlways"));

    private static final Set<String> g1gcOptions =
            new HashSet<>(
                    Arrays.asList(
                            "-XX:+UseG1GC",
                            "-XX:G1HeapRegionSize",
                            "-XX:MaxGCPauseMillis",
                            "-XX:G1NewSizePercent",
                            "-XX:G1MaxNewSizePercent",
                            "-XX:-ResizePLAB",
                            "-XX:InitiatingHeapOccupancyPercent",
                            "-XX:G1MixedGCLiveThresholdPercent",
                            "-XX:G1HeapWastePercent",
                            "-XX:G1MixedGCCountTarget",
                            "-XX:G1OldCSetRegionThresholdPercent",
                            "-XX:G1ReservePercent",
                            "-XX:SoftRefLRUPolicyMSPerMB",
                            "-XX:G1ConcRefinementThreads",
                            "-XX:MaxGCPauseMillis",
                            "-XX:+UnlockExperimentalVMOptions",
                            "-XX:NewRatio",
                            "-XX:G1RSetUpdatingPauseTimePercent"));

    static final GCType getGCType(String option) {
        if (cmsOptions.contains(option)) return GCType.CMS;

        if (g1gcOptions.contains(option)) return GCType.G1GC;

        return null;
    }

    static final GCType getGCType(JVMOption jvmOption) {
        return getGCType(jvmOption.getJvmOption());
    }
}
