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

import com.netflix.priam.scheduler.TaskTimer;

public interface IIncrementalBackup {
	
	public static long INCREMENTAL_INTERVAL_IN_MILLISECS = 10L * 1000;
	
	/*
	 * @return the number of files pending to be uploaded.  The semantic depends on whether the implementation
	 * is synchronous or asynchronous.
	 */
	public long getNumPendingFiles();
	
	public String getJobName();

}