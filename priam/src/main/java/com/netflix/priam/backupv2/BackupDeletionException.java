package com.netflix.priam.backupv2;

public class BackupDeletionException extends Exception {

        private static final long serialVersionUID = 333L;

        public BackupDeletionException(String message) {
            super(message);
        }

        public BackupDeletionException(String message, Exception e) {
            super(message, e);
        }
}