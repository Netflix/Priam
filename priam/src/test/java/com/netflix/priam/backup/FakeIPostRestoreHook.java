package com.netflix.priam.backup;

<<<<<<< Updated upstream
public class FakeIPostRestoreHook {
=======
import com.netflix.priam.restore.IPostRestoreHook;

public class FakeIPostRestoreHook implements IPostRestoreHook {
    public boolean hasValidParameters() {
        return true;
    }

    public void execute() {
        //no op
    }
>>>>>>> Stashed changes
}
