package org.example.executor;

import java.util.function.Supplier;

public class CooperativeThread extends Thread {
    private final CooperativeThreadControl control;

    public CooperativeThread(Runnable target, String name, CooperativeThreadControl control) {
        super(target, name);
        this.control = control;
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

    public static void tryYieldFor(Runnable runnable) {
        tryYieldFor(() -> {
            runnable.run();
            return (Void) null;
        });
    }

    public static <T> T tryYieldFor(Supplier<T> supplier) {
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

    // Inverse!
    public static void tryRequestFor(Runnable runnable) {
        tryRequestFor(() -> {
            runnable.run();
            return (Void) null;
        });
    }

    // Inverse!
    public static <T> T tryRequestFor(Supplier<T> supplier) {
        final CooperativeThreadControl control = getCooperativeThreadControlOrNull();
        if (control == null) {
            return supplier.get();
        }

        control.requestTime();
        try {
            return supplier.get();
        } finally {
            control.releaseTime();
        }
    }
}
