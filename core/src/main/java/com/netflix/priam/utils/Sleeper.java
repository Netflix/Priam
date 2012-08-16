package com.netflix.priam.utils;

/**
 * An abstraction to {@link Thread#sleep(long)} so we can mock it in tests.
 */
public interface Sleeper {
    void sleep(long waitTimeMs) throws InterruptedException;

    ;
}
