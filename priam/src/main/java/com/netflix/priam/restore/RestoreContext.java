package com.netflix.priam.restore;

import com.google.inject.Inject;

/*
 * At run-time, determine the source type to restore from.
 */
public class RestoreContext {

        private IRestoreStrategy restoreObj = null;
        private AwsCrossAccountCryptographyRestoreStrategy awsCrossAccountCryptographyRestoreStrategy;
        private GoogleRestoreStrategy googleRestoreStrategy;

        @Inject
        public RestoreContext(AwsCrossAccountCryptographyRestoreStrategy aws, GoogleRestoreStrategy gcs) {
                this.awsCrossAccountCryptographyRestoreStrategy = aws;
                this.googleRestoreStrategy = gcs;
        }

        public IRestoreStrategy getRestoreObj(SourceType srcType) {
                if (srcType == null) {
                        return null; //not fatal as the client should account for this use case
                }

                if (srcType.equals(SourceType.GOOGLE)) {
                        this.restoreObj =  this.googleRestoreStrategy;
                } else if (srcType.equals(SourceType.AWSCROSSACCTENCRYPTED)){
                        this.restoreObj =  this.awsCrossAccountCryptographyRestoreStrategy;
                }

                return this.restoreObj;
        }

    public enum SourceType {
        AWSCROSSACCTENCRYPTED, GOOGLE
    };


}