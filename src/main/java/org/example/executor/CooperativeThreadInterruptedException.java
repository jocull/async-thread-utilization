package org.example.executor;

public class CooperativeThreadInterruptedException extends CooperativeThreadException {
    public CooperativeThreadInterruptedException(Throwable cause) {
        super(cause);
    }

    public CooperativeThreadInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
