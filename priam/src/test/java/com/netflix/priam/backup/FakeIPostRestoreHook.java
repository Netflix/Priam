package com.netflix.priam.backup;

import com.netflix.priam.restore.IPostRestoreHook;

public class FakeIPostRestoreHook implements IPostRestoreHook {
    public boolean hasValidParameters() {
        return true;
    }

    public void execute() {
        //no op
    }
}
