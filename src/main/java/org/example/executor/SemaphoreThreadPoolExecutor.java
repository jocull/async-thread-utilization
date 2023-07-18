package org.example.executor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SemaphoreThreadPoolExecutor extends ThreadPoolExecutor {
    public SemaphoreThreadPoolExecutor(int poolSize, int parallelism) {
        this(poolSize, new Semaphore(parallelism, true));
    }

    public SemaphoreThreadPoolExecutor(int poolSize, Semaphore semaphore) {
        super(poolSize, poolSize, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new SemaphoreThreadFactory(semaphore));
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        try {
            SemaphoreThread.getSemaphore(t).acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        SemaphoreThread.getSemaphore().release();
        super.afterExecute(r, t);
    }
}
