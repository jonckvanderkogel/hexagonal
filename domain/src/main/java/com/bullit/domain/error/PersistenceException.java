package com.bullit.domain.error;

public final class PersistenceException extends RuntimeException {
    public PersistenceException(String message, Throwable ex) { super(message, ex); }
}
