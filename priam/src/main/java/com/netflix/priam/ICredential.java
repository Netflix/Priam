/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Credential file interface for services supporting 
 * Access ID and key authentication
 */
public interface ICredential
{
    /**
     * @return Access ID
     * @deprecated See {code}getCredentials{code}
     */
    @Deprecated
    public String getAccessKeyId();

    /**
     * @return Secret key
     * @deprecated See {code}getCredentials{code}
     */
    @Deprecated
    public String getSecretAccessKey();

    /**
     * Retrieve AWS credentials (id and key).
     * <p>
     * Added this method to handle potential data races in calling {code}getAccessKeyId{code}
     * and {code}getSecretAccessKey{code} sequentially.
     */
    @Deprecated
    AWSCredentials getCredentials();
    
    /**
     * Retrieve AWS Credential Provider object 
     */
    AWSCredentialsProvider getAwsCredentialProvider();
}
