package com.netflix.priam.agent.process;

import com.google.common.collect.Lists;
import java.util.List;

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
    public List<ArgumentMetaData> getArguments()
    {
        return Lists.newArrayList();
    }
}
