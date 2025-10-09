package com.bullit.domain.error;

public final class DatabaseInteractionException extends RuntimeException {
    public DatabaseInteractionException(String message, Throwable ex) { super(message, ex); }
}
