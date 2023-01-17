/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.utils;

import java.util.Comparator;
import java.util.TreeSet;

public class FifoQueue<E extends Comparable<E>> extends TreeSet<E> {
    private static final long serialVersionUID = -7388604551920505669L;
    private final int capacity;

    public FifoQueue(int capacity) {
        super(Comparator.naturalOrder());
        this.capacity = capacity;
    }

    public FifoQueue(int capacity, Comparator<E> comparator) {
        super(comparator);
        this.capacity = capacity;
    }

    public synchronized void adjustAndAdd(E e) {
        add(e);
        if (capacity < size()) pollFirst();
    }
}
