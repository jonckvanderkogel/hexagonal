package com.bullit.domain.error;

public final class NotFoundException extends RuntimeException {
    public NotFoundException(String message) { super(message); }
}
