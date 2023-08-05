package org.example.cooperative.controllers;

import org.apache.commons.lang3.mutable.MutableInt;
import org.example.cooperative.CooperativeThreadInterruptedException;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/* package-private */
class CooperativeThreadOrderedControl implements CooperativeThreadControl {
    private static final AtomicLong WAIT_ID = new AtomicLong();

    private final ReentrantLock lock = new ReentrantLock(true);
    private final ThreadLocal<ThreadState> threadLocalState = new ThreadLocal<>();
    private final int targetParallelism;
    private final MutableInt currentParallelism; // TODO: Necessary or replace with active set? Tracking waiting threads somewhere?
    private final SortedSet<ThreadState> waiters = new TreeSet<>();

    CooperativeThreadOrderedControl(int parallelism) {
        this.targetParallelism = parallelism;
        this.currentParallelism = new MutableInt(0);
    }

    private ThreadState getThreadState() {
        ThreadState threadState = threadLocalState.get();
        if (threadState == null) {
            threadState = new ThreadState();
            threadLocalState.set(threadState);
        }
        return threadState;
    }

    @Override
    public void startNewTask() {
        final ThreadState threadState = getThreadState();
        threadState.rootTaskId = WAIT_ID.getAndIncrement();
        requestTime();
    }

    @Override
    public void endCurrentTask() {
        final ThreadState threadState = getThreadState();
        releaseTime();
        threadState.rootTaskId = Long.MIN_VALUE;
    }

    @Override
    public void requestTime() {
        final ThreadState threadState = getThreadState();
        final int retainedCount = threadState.retainCounter.incrementAndGet();
        if (retainedCount == 1) {
            try {
                lock.lockInterruptibly();
                // In observations, parallelism could get above the target somehow...
                // This loops helps guard against wake-ups where we aren't actually ready to start
                while (currentParallelism.intValue() >= targetParallelism) {
                    // Wait to be notified when space is free
                    if (!waiters.add(threadState)) {
                        // This should never happen - if it does it's an implementation problem
                        throw new IllegalStateException("Failed to add waiter " + threadState.rootTaskId);
                    }
                    threadState.condition.await(); // TODO: If interrupted, what is the clean-up step?
                }
                currentParallelism.incrementAndGet();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new CooperativeThreadInterruptedException(ex);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void releaseTime() {
        final int retainedCount = getThreadState().retainCounter.decrementAndGet();
        if (retainedCount == 0) {
            lock.lock();
            try {
                currentParallelism.decrementAndGet();
                // Any waiters to give time to?
                if (!waiters.isEmpty()) {
                    final ThreadState w = waiters.first();
                    // Remove the item immediately, so it won't be double signaled by anyone
                    if (!waiters.remove(w)) {
                        // This should never happen - if it does it's an implementation problem
                        throw new IllegalStateException("Failed to remove waiter " + w.rootTaskId);
                    }
                    w.condition.signal();
                }
            } finally {
                lock.unlock();
            }
        } else if (retainedCount < 0) {
            throw new IllegalStateException("Retained count = " + retainedCount);
        }
    }

    private class ThreadState implements Comparable<ThreadState> {
        long rootTaskId;
        final MutableInt retainCounter;
        final Condition condition;

        public ThreadState() {
            this.rootTaskId = 0L;
            this.retainCounter = new MutableInt(0);
            this.condition = lock.newCondition();
        }

        @Override
        public int compareTo(ThreadState o) {
            return Long.compare(rootTaskId, o.rootTaskId);
        }
    }
}
