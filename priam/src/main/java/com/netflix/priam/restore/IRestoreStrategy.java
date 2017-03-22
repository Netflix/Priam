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
package com.netflix.priam.restore;

import java.util.Date;

/*
 * A means to restore C* files from various source types (e.g. Google, AWS bucket whose objects are not owned by the current IAM role), and encrypted / non-encrypted data.
 */
public interface IRestoreStrategy {
	public void restore(Date startTime, Date endTime) throws Exception;
}
