package com.netflix.priam.agent.process;

import javax.inject.Provider;

/**
 * A class based provider
 */
public class SimpleProvider<T> implements Provider<T>
{
    private final Class<T> clazz;

    /**
     * Return a provider for the given class
     *
     * @param clazz class
     * @return provider
     */
    public static <T> Provider<T> of(Class<T> clazz)
    {
        return new SimpleProvider<T>(clazz);
    }

    /**
     * @param clazz Provider class
     */
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
