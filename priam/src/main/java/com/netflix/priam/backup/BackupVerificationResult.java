/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.priam.backup;

import com.netflix.priam.utils.GsonJsonSerializer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by aagrawal on 2/16/17. This class holds the result from BackupVerification. The default
 * are all null and false.
 */
public class BackupVerificationResult {
    public boolean valid = false;
    public String remotePath = null;
    public Instant snapshotInstant = null;
    public boolean manifestAvailable = false;
    public List<String> filesInMetaOnly = new ArrayList<>();
    public int filesMatched = 0;

    @Override
    public String toString() {
        return GsonJsonSerializer.getGson().toJson(this);
    }
}
