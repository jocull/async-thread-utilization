package org.example.executor;

public interface CooperativeThreadControl {
    void requestTime();

    void releaseTime();

    static CooperativeThreadControl create(int parallelism) {
        return new CooperativeThreadSemaphoreControl(parallelism);
    }
}
