package org.example.cooperative.controllers;

public class CooperativeThreadNoOpControl implements CooperativeThreadControl {
    @Override
    public void requestTime() {
        // no-op
    }

    @Override
    public void releaseTime() {
        // no-op
    }
}
