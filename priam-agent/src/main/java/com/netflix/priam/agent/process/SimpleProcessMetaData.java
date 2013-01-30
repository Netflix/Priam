package com.netflix.priam.agent.process;

public class SimpleProcessMetaData implements ProcessMetaData
{
    private final String helpText;

    public SimpleProcessMetaData(String helpText)
    {
        this.helpText = helpText;
    }

    @Override
    public String getHelpText()
    {
        return helpText;
    }

    @Override
    public int getMinArguments()
    {
        return 0;
    }
}
