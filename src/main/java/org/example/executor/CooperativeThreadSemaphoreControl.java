package org.example.executor;

import java.util.concurrent.Semaphore;

class CooperativeThreadSemaphoreControl implements CooperativeThreadControl {
    private final Semaphore semaphore;

    CooperativeThreadSemaphoreControl(int parallelism) {
        this.semaphore = new Semaphore(parallelism, true);
    }

    @Override
    public void requestTime() throws InterruptedException {
        semaphore.acquire();
    }

    @Override
    public void releaseTime() {
        semaphore.release();
    }
}
