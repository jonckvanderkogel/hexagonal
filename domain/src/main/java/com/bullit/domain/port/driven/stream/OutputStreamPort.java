package com.bullit.domain.port.driven.stream;

public interface OutputStreamPort<T> {
    void emit(T element);
    void fireAndForget(T element);
}
