package com.netflix.priam.utils;

import com.netflix.priam.utils.Mutex;
import com.google.common.base.Throwables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Data-center-wide mutex spans all instances of the DataStore for a particular deployment in a particular data center.
 */
public class CuratorMutex implements Mutex {
    private final CuratorFramework _curatorFramework;
    private final String _path;
    private final Counter _acquired;

    public CuratorMutex(CuratorFramework curatorFramework, String path, String group) {
        _curatorFramework = checkNotNull(curatorFramework, "curatorFramework");
        _path = checkNotNull(path, "path");

        // For debugging, report whether or not the app thinks it holds the mutex.  This will return 0 or 1 as long as
        // ZooKeeper is working correctly.
        _acquired = Metrics.newCounter(new MetricName(group, "CuratorMutex", "acquired"));
    }

    @Override
    public Handle acquire(long time, TimeUnit unit) throws TimeoutException {
        final InterProcessMutex mutex = new InterProcessMutex(_curatorFramework, _path);
        boolean success;
        try {
            success = mutex.acquire(time, unit);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        if (!success) {
            throw new TimeoutException();
        }
        _acquired.inc();
        return new Handle() {
            @Override
            public void release() {
                try {
                    _acquired.dec();
                    mutex.release();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
