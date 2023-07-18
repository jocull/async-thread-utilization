package org.example.executor;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SemaphoreThreadFactory implements ThreadFactory {
    private final static AtomicInteger POOL_COUNTER = new AtomicInteger();

    private final Semaphore semaphore;
    private final String poolName;
    private final AtomicInteger threadCounter = new AtomicInteger();

    public SemaphoreThreadFactory(Semaphore semaphore) {
        this.semaphore = semaphore;
        this.poolName = "release-retain-pool-" + POOL_COUNTER.getAndIncrement();
    }

    @Override
    public Thread newThread(Runnable r) {
        return new SemaphoreThread(r, poolName + "-" + threadCounter.getAndIncrement(), semaphore);
    }
}
