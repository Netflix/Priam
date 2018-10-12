package com.netflix.priam.defaultimpl;

import java.io.IOException;

/** Created by aagrawal on 10/3/17. */
public class FakeCassandraProcess implements ICassandraProcess {

    @Override
    public void start(boolean join_ring) throws IOException {
        // do nothing
    }

    @Override
    public void stop(boolean force) throws IOException {
        // do nothing
    }
}
