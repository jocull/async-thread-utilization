package org.example.executor;

public class CooperativeThreadException extends RuntimeException {
    public CooperativeThreadException(Throwable cause) {
        super(cause);
    }

    public CooperativeThreadException(String message, Throwable cause) {
        super(message, cause);
    }
}
