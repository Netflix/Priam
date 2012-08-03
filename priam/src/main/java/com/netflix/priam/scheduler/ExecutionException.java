package com.netflix.priam.scheduler;

public class ExecutionException extends Exception
{
    private static final long serialVersionUID = 1L;
    
    public ExecutionException(String msg, Throwable th)
    {
        super(msg, th);
    }
    
    public ExecutionException(String msg)
    {
        super(msg);
    }
    
    public ExecutionException(Exception ex)
    {
        super(ex);
    }
    
    public ExecutionException(Throwable th)
    {
        super(th);
    }
}
