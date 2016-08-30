package com.groupon.lex.metrics.lib;

import java.util.Optional;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

public abstract class Any3<T, U, V> {
    public Optional<T> get1() { return Optional.empty(); }
    public Optional<U> get2() { return Optional.empty(); }
    public Optional<V> get3() { return Optional.empty(); }

    public static <T, U, V> Any3<T, U, V> create1(@NonNull T v) { return new Any3_1<>(v); }
    public static <T, U, V> Any3<T, U, V> create2(@NonNull U v) { return new Any3_2<>(v); }
    public static <T, U, V> Any3<T, U, V> create3(@NonNull V v) { return new Any3_3<>(v); }

    public abstract <X, Y, Z> Any3<X, Y, Z> map(Function<? super T, X> fn1, Function<? super U, Y> fn2, Function<? super V, Z> fn3);
    public abstract <X> X mapCombine(Function<? super T, ? extends X> fn1, Function<? super U, ? extends X> fn2, Function<? super V, ? extends X> fn3);

    @Override
    public String toString() { return "Any{" + get1().orElse(null) + ", " + get2().orElse(null) + ", " + get3().orElse(null) + "}"; }

    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static final class Any3_1<T, U, V> extends Any3<T, U, V> {
        @NonNull
        private final T v;

        @Override
        public Optional<T> get1() { return Optional.of(v); }

        @Override
        public <X, Y, Z> Any3<X, Y, Z> map(Function<? super T, X> fn1, Function<? super U, Y> fn2, Function<? super V, Z> fn3) {
            return new Any3_1(fn1.apply(v));
        }

        @Override
        public <X> X mapCombine(Function<? super T, ? extends X> fn1, Function<? super U, ? extends X> fn2, Function<? super V, ? extends X> fn3) {
            return fn1.apply(v);
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static final class Any3_2<T, U, V> extends Any3<T, U, V> {
        @NonNull
        private final U v;

        @Override
        public Optional<U> get2() { return Optional.of(v); }

        @Override
        public <X, Y, Z> Any3<X, Y, Z> map(Function<? super T, X> fn1, Function<? super U, Y> fn2, Function<? super V, Z> fn3) {
            return new Any3_2(fn2.apply(v));
        }

        @Override
        public <X> X mapCombine(Function<? super T, ? extends X> fn1, Function<? super U, ? extends X> fn2, Function<? super V, ? extends X> fn3) {
            return fn2.apply(v);
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static final class Any3_3<T, U, V> extends Any3<T, U, V> {
        @NonNull
        private final V v;

        @Override
        public Optional<V> get3() { return Optional.of(v); }

        @Override
        public <X, Y, Z> Any3<X, Y, Z> map(Function<? super T, X> fn1, Function<? super U, Y> fn2, Function<? super V, Z> fn3) {
            return new Any3_3(fn3.apply(v));
        }

        @Override
        public <X> X mapCombine(Function<? super T, ? extends X> fn1, Function<? super U, ? extends X> fn2, Function<? super V, ? extends X> fn3) {
            return fn3.apply(v);
        }
    }
}
