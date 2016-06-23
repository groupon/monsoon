package com.groupon.lex.metrics.lib;

import static java.util.Objects.requireNonNull;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Lazy evaluation wrapper.
 *
 * Given a supplier (or function with arguments) it will:
 * - invoke the function exactly once to retrieve the result (upon first access)
 * - use the retrieved value on subsequence access.
 *
 * The exception is if the argument throws an exception or is null.
 * @author ariane
 */
public class LazyEval<T> implements Supplier<T> {
    private Supplier<? extends T> actual_;
    private volatile T value_ = null;

    public static <T> LazyEval<T> byValue(T v) {
        LazyEval<T> result = new LazyEval<>(() -> v);
        result.value_ = v;
        return result;
    }

    public LazyEval(Supplier<? extends T> actual) {
        actual_ = requireNonNull(actual);
    }

    public <X> LazyEval(Function<? super X, ? extends T> fn, X arg) {
        this(() -> fn.apply(arg));
    }

    public <X, Y> LazyEval(BiFunction<? super X, ? super Y, ? extends T> fn, X x, Y y) {
        this(() -> fn.apply(x, y));
    }

    public <X, Y, Z> LazyEval(TriFunction<? super X, ? super Y, ? super Z, ? extends T> fn, X x, Y y, Z z) {
        this(() -> fn.apply(x, y, z));
    }

    @Override
    public T get() {
        if (value_ == null) {
            synchronized(this) {
                if (value_ == null) {
                    value_ = actual_.get();
                    if (value_ != null) actual_ = null;  // Drop dependant data when no longer needed.
                }
            }
        }
        return value_;
    }
}
