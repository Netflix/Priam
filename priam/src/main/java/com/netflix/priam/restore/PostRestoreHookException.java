package com.netflix.priam.restore;

<<<<<<< Updated upstream
public class PostRestoreHookException {
}
=======
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
>>>>>>> Stashed changes
