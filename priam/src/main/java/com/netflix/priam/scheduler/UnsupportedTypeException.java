package com.netflix.priam.scheduler;

/**
 * Created by aagrawal on 3/14/17.
 */
public class UnsupportedTypeException extends Exception {
    public UnsupportedTypeException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public UnsupportedTypeException(String msg)
    {
        super(msg);
    }

    public UnsupportedTypeException(Exception ex)
    {
        super(ex);
    }

    public UnsupportedTypeException(Throwable th)
    {
        super(th);
    }
}
