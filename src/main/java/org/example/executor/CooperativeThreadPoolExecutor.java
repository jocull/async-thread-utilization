package org.example.executor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CooperativeThreadPoolExecutor extends ThreadPoolExecutor {
    public CooperativeThreadPoolExecutor(int poolSize, int parallelism) {
        this(poolSize, CooperativeThreadControl.create(parallelism));
    }

    public CooperativeThreadPoolExecutor(int poolSize, CooperativeThreadControl control) {
        super(poolSize, poolSize, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new CooperativeThreadFactory("cooperative-pool", control));
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        final CooperativeThread ct = (CooperativeThread) t;
        ct.setRootTaskTime(System.currentTimeMillis());
        ct.getControl().requestTime(ct);
        super.beforeExecute(ct, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        final CooperativeThread ct = (CooperativeThread) Thread.currentThread();
        ct.clearRootTaskTime();
        ct.getControl().releaseTime(ct);
        super.afterExecute(r, t);
    }
}
