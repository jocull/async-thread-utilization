package org.example.cooperative.controllers;

import org.example.cooperative.CooperativeThread;

public class CooperativeThreadNoOpControl implements CooperativeThreadControl {
    @Override
    public void requestTime(CooperativeThread ct) {
        // no-op
    }

    @Override
    public void releaseTime(CooperativeThread ct) {
        // no-op
    }
}
