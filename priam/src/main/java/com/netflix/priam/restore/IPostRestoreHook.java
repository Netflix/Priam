package com.netflix.priam.restore;

<<<<<<< Updated upstream
public class IPostRestoreHook {
=======
import com.google.inject.ImplementedBy;

/**
 * Interface for post restore hook
 */
@ImplementedBy(PostRestoreHook.class)
public interface IPostRestoreHook {
    boolean hasValidParameters();
    void execute() throws Exception;
>>>>>>> Stashed changes
}
