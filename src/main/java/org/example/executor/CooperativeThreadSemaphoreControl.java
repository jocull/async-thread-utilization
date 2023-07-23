package org.example.executor;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.concurrent.Semaphore;

class CooperativeThreadSemaphoreControl implements CooperativeThreadControl {
    private final Semaphore semaphore;
    private final ThreadLocal<MutableInt> threadRetainCounter = new ThreadLocal<>(); // TODO: Should this be a field of the thread itself?

    CooperativeThreadSemaphoreControl(int parallelism) {
        this.semaphore = new Semaphore(parallelism, true);
    }

    private MutableInt getThreadRetainCounter() {
        MutableInt mutableInt = threadRetainCounter.get();
        if (mutableInt == null) {
            mutableInt = new MutableInt(0);
            threadRetainCounter.set(mutableInt);
        }
        return mutableInt;
    }

    @Override
    public void requestTime() {
        final int retainedCount = getThreadRetainCounter().incrementAndGet();
        if (retainedCount == 1) {
            try {
                semaphore.acquire();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new CooperativeThreadInterruptedException(ex);
            }
        }
    }

    @Override
    public void releaseTime() {
        final int retainedCount = getThreadRetainCounter().decrementAndGet();
        if (retainedCount == 0) {
            semaphore.release();
        } else if (retainedCount < 0) {
            throw new IllegalStateException("Cooperative thread retained count = " + retainedCount);
        }
    }
}
