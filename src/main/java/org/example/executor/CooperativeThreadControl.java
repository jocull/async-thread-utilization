package org.example.executor;

public interface CooperativeThreadControl {
    void requestTime(CooperativeThread ct);

    void releaseTime(CooperativeThread ct);

    static CooperativeThreadControl create(int parallelism) {
        return new CooperativeThreadOrderedControl(parallelism);
    }
}
