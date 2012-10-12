package com.netflix.priam.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Mutex {
    Handle acquire(long time, TimeUnit unit) throws TimeoutException;

    public interface Handle {
        void release();
    }
}
