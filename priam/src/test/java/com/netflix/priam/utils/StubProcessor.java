package com.netflix.priam.utils;

import com.netflix.priam.ICassandraProcess;

import java.io.IOException;

public class StubProcessor implements ICassandraProcess
{
    @Override
    public void start(boolean join_ring) throws IOException
    {

    }

    @Override
    public void stop() throws IOException
    {

    }
}
