/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.scheduler;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import javax.inject.Singleton;
import org.junit.Test;

public class TestGuiceSingleton {
    @Test
    public void testSingleton() {
        Injector injector = Guice.createInjector(new GModules());
        injector.getInstance(EmptryInterface.class).print();
        injector.getInstance(EmptryInterface.class).print();
        injector.getInstance(EmptryInterface.class).print();
        printInjected();
        printInjected();
        printInjected();
        printInjected();
    }

    private void printInjected() {
        Injector injector = Guice.createInjector(new GModules());
        injector.getInstance(EmptryInterface.class).print();
    }

    interface EmptryInterface {
        void print();
    }

    @Singleton
    public static class GuiceSingleton implements EmptryInterface {

        public void print() {
            System.out.println(this.toString());
            this.toString();
        }
    }

    static class GModules extends AbstractModule {
        @Override
        protected void configure() {
            bind(EmptryInterface.class).to(GuiceSingleton.class).asEagerSingleton();
        }
    }
}
