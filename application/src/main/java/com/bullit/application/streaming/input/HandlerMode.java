package com.bullit.application.streaming.input;

import com.bullit.domain.port.driving.stream.BatchStreamHandler;
import com.bullit.domain.port.driving.stream.StreamHandler;

import java.util.List;

sealed interface HandlerMode<T> permits HandlerMode.Single, HandlerMode.Batch {
    int maxBatchSize();

    void handle(List<T> payloads);

    record Single<T>(StreamHandler<T> handler) implements HandlerMode<T> {
        @Override
        public int maxBatchSize() {
            return 1;
        }

        @Override
        public void handle(List<T> payloads) {
            payloads.forEach(handler::handle);
        }
    }

    record Batch<T>(BatchStreamHandler<T> handler, int maxBatchSize) implements HandlerMode<T> {
        @Override
        public void handle(List<T> payloads) {
            handler.handleBatch(payloads);
        }
    }

    static <T> HandlerMode<T> single(StreamHandler<T> handler) {
        return new Single<>(handler);
    }

    static <T> HandlerMode<T> batch(BatchStreamHandler<T> handler, int maxBatchSize) {
        if (maxBatchSize <= 0) throw new IllegalArgumentException("maxBatchSize must be > 0");
        return new Batch<>(handler, maxBatchSize);
    }
}
