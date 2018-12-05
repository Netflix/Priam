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

package com.netflix.priam.utils;

import org.junit.Assert;
import org.junit.Test;

/** Created by aagrawal on 12/1/18. */
public class TestSystemUtils {

    @Test
    public void testGetDataFromUrl() {
        String dummyurl = "https://jsonplaceholder.typicode.com/todos/1";
        String response = SystemUtils.getDataFromUrl(dummyurl);
        Assert.assertNotNull(response);
    }
}
