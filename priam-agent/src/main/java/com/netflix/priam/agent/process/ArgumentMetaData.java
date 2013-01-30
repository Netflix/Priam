package com.netflix.priam.agent.process;

public interface ArgumentMetaData
{
    public String getName();

    public String getDescription();

    public boolean isVariableLength();

    public boolean isOptional();
}
