package com.netflix.priam.backupv2;

public enum Failure {
    APP_PREFIX_IS_INVALID("appPrefixIsInvalid"),
    EMPTY_META_FILE("emptyMetaFile"),
    FAILED_DELETING_FILES("failedDeletingFiles"),
    FAILED_READING_META_FILE("failedReadingMetaFile"),
    FILES_LEFT_OVER("filesLeftOver"),
    GENERAL_FAILURE("generalFailure"),
    NO_PREFIXES_FOUND("noAppPrefixesFound"),
    NO_META_FILES("noMetaFiles"),
    NO_USABLE_META_FILE("noUsableMetaFile"),
    TOKEN_PREFIX_IS_INVALID("tokenPrefixIsInvalid")

    private String registryKey;

    Failure(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getRegistryKey() {
        return registryKey;
    }
}
