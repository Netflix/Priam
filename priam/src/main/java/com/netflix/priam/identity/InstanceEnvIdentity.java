/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.identity;

/*
 * A means to determine the environment for the running instance
 */
public interface InstanceEnvIdentity {
    /*
     * @return true if running instance is in "classic", false otherwise.
     */
    Boolean isClassic();

    /*
     * @return true if running instance is in vpc, under your default AWS account, false otherwise.
     */
    Boolean isDefaultVpc();

    /*
     * @return true if running instance is in vpc, under a specific AWS account, false otherwise.
     */
    Boolean isNonDefaultVpc();

    enum InstanceEnvironent {
        CLASSIC, DEFAULT_VPC, NONDEFAULT_VPC
    }

}
