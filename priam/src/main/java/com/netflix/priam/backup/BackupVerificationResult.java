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

import java.util.List;

/**
 * Created by aagrawal on 2/16/17.
 * This class holds the result from BackupVerification. The default are all null and false.
 */

public class BackupVerificationResult
{
    public boolean snapshotAvailable = false;
    public boolean valid = false;
    public boolean metaFileFound = false;
    public boolean backupFileListAvail = false;
    public String selectedDate = null;
    public String snapshotTime = null;
    public List<String> filesInMetaOnly = null;
    public List<String> filesInS3Only = null;
    public List<String> filesMatched = null;
}