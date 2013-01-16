package com.netflix.priam.agent.process;

import javax.inject.Provider;

public class SimpleProvider<T> implements Provider<T>
{
    private final Class<T> clazz;

    public static <T> SimpleProvider<T> of(Class<T> clazz)
    {
        return new SimpleProvider<T>(clazz);
    }

    public SimpleProvider(Class<T> clazz)
    {
        this.clazz = clazz;
    }

    @Override
    public T get()
    {
        try
        {
            return clazz.newInstance();
        }
        catch ( InstantiationException e )
        {
            throw new RuntimeException(e);
        }
        catch ( IllegalAccessException e )
        {
            throw new RuntimeException(e);
        }
    }
}
