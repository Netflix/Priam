package com.netflix.priam.restore;

/**
 * Exception raised by PostRestoreHook
 */
public class PostRestoreHookException extends Exception {

    public PostRestoreHookException(String message) {
        super(message);
    }

    public PostRestoreHookException(String message, Exception e) {
        super(message, e);
    }

}
