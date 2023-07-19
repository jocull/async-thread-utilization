package org.example.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CooperativeThreadFactory implements ThreadFactory {
    private final static AtomicInteger POOL_COUNTER = new AtomicInteger();

    private final CooperativeThreadControl control;
    private final String poolName;
    private final AtomicInteger threadCounter = new AtomicInteger();

    public CooperativeThreadFactory(String threadPrefix, CooperativeThreadControl control) {
        this.control = control;
        this.poolName = threadPrefix + "-" + POOL_COUNTER.getAndIncrement();
    }

    @Override
    public Thread newThread(Runnable r) {
        return new CooperativeThread(r, poolName + "-" + threadCounter.getAndIncrement(), control);
    }
}
