package org.example.executor;

import java.util.concurrent.*;

public class CooperativeThreadPoolExecutor extends ThreadPoolExecutor {
    public CooperativeThreadPoolExecutor(int poolSize, int parallelism) {
        this(poolSize, CooperativeThreadControl.create(parallelism));
    }

    public CooperativeThreadPoolExecutor(int poolSize, CooperativeThreadControl control) {
        super(poolSize, poolSize, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new CooperativeThreadFactory("cooperative-pool", control));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new RootTimedFutureTask<>(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new RootTimedFutureTask<>(callable);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        final CooperativeThread ct = (CooperativeThread) t;
        ct.setRootTaskTime(((RootTimedFutureTask<?>) r).createTime);
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

    private static class RootTimedFutureTask<T> extends FutureTask<T> {
        final long createTime; // TODO: Move all task ID logic to here?

        public RootTimedFutureTask(Callable<T> callable) {
            super(callable);
            this.createTime = System.currentTimeMillis();
        }

        public RootTimedFutureTask(Runnable runnable, T result) {
            super(runnable, result);
            this.createTime = System.currentTimeMillis();
        }
    }
}
