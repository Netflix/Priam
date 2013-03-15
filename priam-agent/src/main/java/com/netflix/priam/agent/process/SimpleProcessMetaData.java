package com.netflix.priam.agent.process;

/**
 * Concrete imp for ProcessMetaData
 */
public class SimpleProcessMetaData implements ProcessMetaData
{
    private final String helpText;
    private final int minArguments;

    public SimpleProcessMetaData(String helpText)
    {
        this(helpText, 0);
    }

    public SimpleProcessMetaData(String helpText, int minArguments)
    {
        this.helpText = helpText;
        this.minArguments = minArguments;
    }

    @Override
    public String getHelpText()
    {
        return helpText;
    }

    @Override
    public int getMinArguments()
    {
        return minArguments;
    }
}
