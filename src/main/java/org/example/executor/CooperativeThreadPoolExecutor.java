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
        CooperativeThread.getCooperativeThreadControlOrThrow(t).requestTime();
        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        CooperativeThread.getCooperativeThreadControlOrThrow().releaseTime();
        super.afterExecute(r, t);
    }
}
