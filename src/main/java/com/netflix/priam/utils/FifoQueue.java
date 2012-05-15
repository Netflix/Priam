package com.netflix.priam.utils;

import java.util.Comparator;
import java.util.TreeSet;

public class FifoQueue<E extends Comparable<E>> extends TreeSet<E>
{
    private static final long serialVersionUID = -7388604551920505669L;
    private int capacity;

    public FifoQueue(int capacity)
    {
        super(new Comparator<E>()
        {
            @Override
            public int compare(E o1, E o2)
            {
                return o1.compareTo(o2);
            }
        });
        this.capacity = capacity;
    }

    public FifoQueue(int capacity, Comparator<E> comparator)
    {
        super(comparator);
        this.capacity = capacity;
    }

    public synchronized void adjustAndAdd(E e)
    {
        add(e);
        if (capacity < size())
            pollFirst();
    }
}
