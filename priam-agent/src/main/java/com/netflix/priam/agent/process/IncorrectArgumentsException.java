package com.netflix.priam.agent.process;

/**
 * Thrown when an attempt is made to start a process with the incorrect number of arguments
 */
public class IncorrectArgumentsException extends Exception
{
    public IncorrectArgumentsException(String message)
    {
        super(message);
    }
}
