package org.example.cooperative.controllers;

import org.example.cooperative.CooperativeThreadInterruptedException;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/* package-private */
class CooperativeThreadOrderedControl implements CooperativeThreadControl {
    private static final AtomicLong ROOT_TASK_ID_COUNTER = new AtomicLong();
    private static final long NO_TASK_ID = Long.MIN_VALUE;

    private final ReentrantLock lock = new ReentrantLock(true);
    private final ThreadLocal<ThreadState> threadLocalState = new ThreadLocal<>();
    private final int targetParallelism;
    private int currentParallelism = 0;
    private final SortedSet<ThreadState> waiters = new TreeSet<>();

    CooperativeThreadOrderedControl(int parallelism) {
        this.targetParallelism = parallelism;
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
    public void requestTime() {
        final ThreadState threadState = getThreadState();
        try {
            lock.lockInterruptibly();
            if (threadState.retainCounter == 0) {
                // When acquiring the thread for the first time, set the root task ID
                if (threadState.rootTaskId == NO_TASK_ID) {
                    threadState.rootTaskId = ROOT_TASK_ID_COUNTER.getAndIncrement();
                }

                // In observations, parallelism could get above the target somehow...
                // This loops helps guard against wake-ups where we aren't actually ready to start
                while (currentParallelism >= targetParallelism) {
                    // Wait to be notified when space is free
                    if (!waiters.add(threadState)) {
                        // This should never happen - if it does it's an implementation problem
                        throw new IllegalStateException("Failed to add waiter " + threadState.rootTaskId);
                    }
                    try {
                        threadState.condition.await(); // If interrupted, throws before incrementing retain counts
                    } catch (InterruptedException ex) {
                        // Interrupted before we could acquire the thread.
                        // Wipe out the root task time and propagate the error.
                        threadState.rootTaskId = NO_TASK_ID;
                        throw ex;
                    }
                }
                // Thread now held
                currentParallelism++;
            }
            // Deepen the retain value
            threadState.retainCounter++;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CooperativeThreadInterruptedException(ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void releaseTime() {
        final ThreadState threadState = getThreadState();
        lock.lock();
        try {
            if (threadState.retainCounter >= 1) {
                threadState.retainCounter--;

                // When there are no retainers left, give up the thread
                if (threadState.retainCounter == 0) {
                    currentParallelism--;
                    threadState.rootTaskId = NO_TASK_ID;

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
                }
            } else if (threadState.retainCounter < 0) {
                throw new IllegalStateException("Tried to release a thread that was never retained. " +
                        "Retained count = " + threadState.retainCounter);
            }
        } finally {
            lock.unlock();
        }
    }

    private class ThreadState implements Comparable<ThreadState> {
        long rootTaskId;
        int retainCounter;
        final Condition condition;

        public ThreadState() {
            this.rootTaskId = NO_TASK_ID;
            this.retainCounter = 0;
            this.condition = lock.newCondition();
        }

        @Override
        public int compareTo(ThreadState o) {
            return Long.compare(rootTaskId, o.rootTaskId);
        }
    }
}
