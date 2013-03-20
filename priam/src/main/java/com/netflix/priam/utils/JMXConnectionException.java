package com.netflix.priam.utils;

import java.io.IOException;

public class JMXConnectionException extends IOException
{

    private static final long serialVersionUID = 444L;

    public JMXConnectionException(String message)
    {
        super(message);
    }

    public JMXConnectionException(String message, Exception e)
    {
        super(message, e);
    }

}
