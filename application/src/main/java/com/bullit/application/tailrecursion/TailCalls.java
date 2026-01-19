package com.bullit.application.tailrecursion;

public class TailCalls {
    public enum Unit {
        INSTANCE
    }

    public static TailCall<Unit> done() {
        return done(Unit.INSTANCE);
    }

    public static <T> TailCall<T> done(final T value) {
        return new TailCall<T>() {
            @Override
            public boolean isComplete() {
                return true;
            }

            @Override
            public T result() {
                return value;
            }

            @Override
            public TailCall<T> apply() {
                throw new Error("not implemented");
            }
        };
    }
}
