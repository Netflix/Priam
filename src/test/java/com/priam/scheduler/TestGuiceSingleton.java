package com.priam.scheduler;

import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

public class TestGuiceSingleton
{
    @Test
    public void testSingleton()
    {
        Injector injector = Guice.createInjector(new GModules());
        injector.getInstance(EmptryInterface.class).print();
        injector.getInstance(EmptryInterface.class).print();
        injector.getInstance(EmptryInterface.class).print();
        printInjected();
        printInjected();
        printInjected();
        printInjected();
    }
    
    public void printInjected()
    {
        Injector injector = Guice.createInjector(new GModules());
        injector.getInstance(EmptryInterface.class).print();
    }

    public interface EmptryInterface
    {
        public String print();
    }

    @Singleton
    public static class GuiceSingleton implements EmptryInterface
    {

        public String print()
        {
            System.out.println(this.toString());
            return this.toString();
        }
    }

    public static class GModules extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind(EmptryInterface.class).to(GuiceSingleton.class).asEagerSingleton();
        }

    }
}
