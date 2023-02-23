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
package com.netflix.priam.scheduler;

/** Created by aagrawal on 3/14/17. */
public class UnsupportedTypeException extends Exception {
    public UnsupportedTypeException(String msg, Throwable th) {
        super(msg, th);
    }

    public UnsupportedTypeException(String msg) {
        super(msg);
    }

    public UnsupportedTypeException(Exception ex) {
        super(ex);
    }

    public UnsupportedTypeException(Throwable th) {
        super(th);
    }
}
