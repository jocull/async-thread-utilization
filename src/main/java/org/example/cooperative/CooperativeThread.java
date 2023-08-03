package org.example.cooperative;

import org.example.cooperative.controllers.CooperativeThreadControl;

import java.util.function.Supplier;

@Deprecated // TODO: Can this be replaced entirely with ThreadLocals?
public class CooperativeThread extends Thread {
    private final CooperativeThreadControl control;
    private volatile long rootTaskTime;

    /* package-private */
    CooperativeThread(Runnable target, String name, CooperativeThreadControl control) {
        super(target, name);
        this.control = control;
    }

    public CooperativeThreadControl getControl() {
        return control;
    }

    public long getRootTaskTime() {
        return rootTaskTime;
    }

    /* package-private */
    void setRootTaskTime(long rootTaskTime) {
        this.rootTaskTime = rootTaskTime;
    }

    /* package-private */
    void clearRootTaskTime() {
        this.rootTaskTime = Long.MIN_VALUE;
    }

    /* package-private */
    static CooperativeThread getCooperativeThreadOrNull() {
        return getCooperativeThreadOrNull(Thread.currentThread());
    }

    /* package-private */
    static CooperativeThread getCooperativeThreadOrNull(Thread thread) {
        if (thread instanceof CooperativeThread) {
            return (CooperativeThread) thread;
        }
        return null;
    }

    /* package-private */
    static CooperativeThread getCooperativeThreadOrThrow() {
        return getCooperativeThreadOrThrow(Thread.currentThread());
    }

    /* package-private */
    static CooperativeThread getCooperativeThreadOrThrow(Thread thread) {
        if (thread instanceof CooperativeThread) {
            return (CooperativeThread) thread;
        }
        throw new IllegalStateException("CooperativeThread not found in "
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
        final CooperativeThread ct = getCooperativeThreadOrNull();
        if (ct == null) {
            return supplier.get();
        }

        ct.getControl().releaseTime(ct);
        try {
            return supplier.get();
        } finally {
            ct.getControl().requestTime(ct);
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
        final CooperativeThread ct = getCooperativeThreadOrNull();
        if (ct == null) {
            return supplier.get();
        }

        ct.control.requestTime(ct);
        try {
            return supplier.get();
        } finally {
            ct.control.releaseTime(ct);
        }
    }
}
