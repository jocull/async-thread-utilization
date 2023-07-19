package org.example.executor;

public interface CooperativeThreadControl {
    void requestTime() throws InterruptedException;

    void releaseTime();

    static CooperativeThreadControl create(int parallelism) {
        return new CooperativeThreadSemaphoreControl(parallelism);
    }
}
