package com.netflix.priam.restore;

import com.google.inject.ImplementedBy;

/**
 * Interface for post restore hook
 */
@ImplementedBy(PostRestoreHook.class)
public interface IPostRestoreHook {
    boolean hasValidParameters();
    void execute() throws Exception;
}
