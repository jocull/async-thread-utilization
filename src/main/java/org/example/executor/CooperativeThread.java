package org.example.executor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CooperativeThread extends Thread {
    private final CooperativeThreadControl control;

    public CooperativeThread(Runnable target, String name, CooperativeThreadControl control) {
        super(target, name);
        this.control = control;
    }

    public interface InterruptibleRunnable {
        void run() throws InterruptedException;
    }

    public interface InterruptibleSupplier<T> {
        T get() throws InterruptedException;
    }

    /* package-private */
    static CooperativeThreadControl getCooperativeThreadControlOrNull() {
        return getCooperativeThreadControlOrNull(Thread.currentThread());
    }

    /* package-private */
    static CooperativeThreadControl getCooperativeThreadControlOrNull(Thread thread) {
        if (thread instanceof CooperativeThread) {
            return ((CooperativeThread) thread).control;
        }
        return null;
    }

    /* package-private */
    static CooperativeThreadControl getCooperativeThreadControlOrThrow() {
        return getCooperativeThreadControlOrThrow(Thread.currentThread());
    }

    /* package-private */
    static CooperativeThreadControl getCooperativeThreadControlOrThrow(Thread thread) {
        if (thread instanceof CooperativeThread) {
            return ((CooperativeThread) thread).control;
        }
        throw new IllegalStateException("CooperativeThreadControl not found in "
                + thread.getClass().getName()
                + " \"" + thread.getName() + "\"");
    }

    // TODO: Would need to implement similar methods for other overloads...
    public static <T> T tryYieldFor(InterruptibleSupplier<T> supplier) throws InterruptedException {
        final CooperativeThreadControl control = getCooperativeThreadControlOrNull();
        if (control == null) {
            return supplier.get();
        }

        control.releaseTime();
        try {
            return supplier.get();
        } finally {
            control.requestTime();
        }
    }

    public static void yieldFor(InterruptibleRunnable runnable) throws InterruptedException {
        final CooperativeThreadControl control = getCooperativeThreadControlOrThrow();
        control.releaseTime();
        try {
            runnable.run();
        } finally {
            control.requestTime();
        }
    }

    public static <T> T yieldFor(InterruptibleSupplier<T> supplier) throws InterruptedException {
        final CooperativeThreadControl control = getCooperativeThreadControlOrThrow();
        control.releaseTime();
        try {
            return supplier.get();
        } finally {
            control.requestTime();
        }
    }

    public static <T> T yieldFor(Future<T> future) throws InterruptedException, ExecutionException {
        final CooperativeThreadControl control = getCooperativeThreadControlOrThrow();
        control.releaseTime();
        try {
            return future.get();
        } finally {
            control.requestTime();
        }
    }

    public static <T> T yieldFor(CompletableFuture<T> future) throws InterruptedException {
        final CooperativeThreadControl control = getCooperativeThreadControlOrThrow();
        control.releaseTime();
        try {
            return future.join();
        } finally {
            control.requestTime();
        }
    }
}
