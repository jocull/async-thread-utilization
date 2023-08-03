package org.example.cooperative.controllers;

import org.example.cooperative.CooperativeThread;

public interface CooperativeThreadControl {
    void requestTime(CooperativeThread ct);

    void releaseTime(CooperativeThread ct);

    static CooperativeThreadControl create(int parallelism) {
        return new CooperativeThreadOrderedControl(parallelism);
    }
}
