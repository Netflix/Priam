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

                if (srcType.equals(SourceType.GOOGLEENCRYPTED)) {
                        this.restoreObj =  this.googleCryptographyRestoreStrategy;
                } else if (srcType.equals(SourceType.AWSCROSSACCTENCRYPTED)){
                        this.restoreObj =  this.awsCrossAccountCryptographyRestoreStrategy;
                }

                return this.restoreObj;
        }

    public enum SourceType {
        AWSCROSSACCTENCRYPTED, GOOGLEENCRYPTED
    };


}