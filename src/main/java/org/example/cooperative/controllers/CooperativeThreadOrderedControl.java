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
    private final MutableInt currentParallelism; // Necessary or replace with active set? Tracking waiting threads somewhere?
    private final SortedSet<CooperativeThreadWaiter> waiters = new TreeSet<>();

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
        threadState.rootTaskTime = System.currentTimeMillis();
        requestTime();
    }

    @Override
    public void endCurrentTask() {
        final ThreadState threadState = getThreadState();
        releaseTime();
        threadState.rootTaskTime = 0L;
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
                    final CooperativeThreadWaiter w = new CooperativeThreadWaiter(WAIT_ID.getAndIncrement(), threadState.rootTaskTime, threadState.condition);
                    if (!waiters.add(w)) {
                        // This should never happen - if it does it's an implementation problem
                        throw new IllegalStateException("Failed to add waiter " + w.waitId);
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
                    final CooperativeThreadWaiter w = waiters.first();
                    // Remove the item immediately, so it won't be double signaled by anyone
                    if (!waiters.remove(w)) {
                        // This should never happen - if it does it's an implementation problem
                        throw new IllegalStateException("Failed to remove waiter " + w.waitId);
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

    private static class CooperativeThreadWaiter implements Comparable<CooperativeThreadWaiter> {
        final long waitId;
        final long waitTime; // TODO: Consider replacing this with a global root task counter?
        final Condition condition;

        public CooperativeThreadWaiter(long waitId, long waitTime, Condition condition) {
            this.waitId = waitId;
            this.waitTime = waitTime;
            this.condition = condition;
        }

        @Override
        public int compareTo(CooperativeThreadWaiter o) {
            final int waitComp = Long.compare(this.waitTime, o.waitTime);
            if (waitComp != 0) {
                return waitComp;
            }
            // Necessary to make tasks unique within the TreeSet.
            // It cannot function without this as it will treat tasks added
            // in the same millisecond as identical!
            return Long.compare(this.waitId, o.waitId);
        }
    }

    private class ThreadState {
        long rootTaskTime;
        final MutableInt retainCounter;
        final Condition condition;

        public ThreadState() {
            this.rootTaskTime = 0L;
            this.retainCounter = new MutableInt(0);
            this.condition = lock.newCondition();
        }
    }
}
