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

package com.netflix.priam.backup.identity.token;

import com.google.common.collect.ListMultimap;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.token.IDeadTokenRetriever;

class FakeDeadTokenRetriever implements IDeadTokenRetriever {

    @Override
    public PriamInstance get() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getReplaceIp() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setLocMap(ListMultimap<String, PriamInstance> locMap) {
        // TODO Auto-generated method stub

    }
}
