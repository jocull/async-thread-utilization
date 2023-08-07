package org.example.cooperative.controllers;

import java.util.function.Supplier;

public interface CooperativeThreadControl {
    void startNewTask();

    void endCurrentTask();

    default void runTask(Runnable runnable) {
        runTask(() -> {
            runnable.run();
            return (Void) null;
        });
    }

    default <T> T runTask(Supplier<T> supplier) {
        startNewTask();
        requestTime();
        try {
            return supplier.get();
        } finally {
            releaseTime();
            endCurrentTask();
        }
    }

    void requestTime();

    void releaseTime();

    default void tryYieldFor(Runnable runnable) {
        tryYieldFor(() -> {
            runnable.run();
            return (Void) null;
        });
    }

    default <T> T tryYieldFor(Supplier<T> supplier) {
        releaseTime();
        try {
            return supplier.get();
        } finally {
            requestTime();
        }
    }

    default void tryRequestFor(Runnable runnable) {
        tryRequestFor(() -> {
            runnable.run();
            return (Void) null;
        });
    }

    // Inverse!
    default <T> T tryRequestFor(Supplier<T> supplier) {
        requestTime();
        try {
            return supplier.get();
        } finally {
            releaseTime();
        }
    }

    static CooperativeThreadControl create(int parallelism) {
        return new CooperativeThreadOrderedControl(parallelism);
    }

    static CooperativeThreadControl none() {
        return new CooperativeThreadNoOpControl();
    }
}
