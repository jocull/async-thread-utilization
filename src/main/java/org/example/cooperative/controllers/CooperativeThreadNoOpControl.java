package org.example.cooperative.controllers;

class CooperativeThreadNoOpControl implements CooperativeThreadControl {
    @Override
    public void startNewTask() {
        // no-op
    }

    @Override
    public void endCurrentTask() {
        // no-op
    }

    @Override
    public void requestTime() {
        // no-op
    }

    @Override
    public void releaseTime() {
        // no-op
    }
}
