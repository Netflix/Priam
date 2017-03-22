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

import com.google.inject.Inject;

/*
 * At run-time, determine the source type to restore from.
 */
public class RestoreContext {

        private IRestoreStrategy restoreObj = null;
        private AwsCrossAccountCryptographyRestoreStrategy awsCrossAccountCryptographyRestoreStrategy;
        private GoogleCryptographyRestoreStrategy googleCryptographyRestoreStrategy;

        @Inject
        public RestoreContext(AwsCrossAccountCryptographyRestoreStrategy aws, GoogleCryptographyRestoreStrategy gcs) {
                this.awsCrossAccountCryptographyRestoreStrategy = aws;
                this.googleCryptographyRestoreStrategy = gcs;
        }

        public IRestoreStrategy getRestoreObj(SourceType srcType) {
                if (srcType == null) {
                        return null; //not fatal as the client should account for this use case
                }

                if (srcType.equals(SourceType.GOOGLE)) {
                        this.restoreObj =  this.googleCryptographyRestoreStrategy;
                } else if (srcType.equals(SourceType.AWSCROSSACCT)){
                        this.restoreObj =  this.awsCrossAccountCryptographyRestoreStrategy;
                }

                return this.restoreObj;
        }

    public enum SourceType {
        AWSCROSSACCT, GOOGLE
    };


}