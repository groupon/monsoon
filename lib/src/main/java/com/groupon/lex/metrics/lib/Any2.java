/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.lib;

import java.util.Optional;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 *
 * @author ariane
 */
public abstract class Any2<T, U> {
    private Any2() {}

    public static <T, U> Any2<T, U> left(@NonNull T t) {
        return new LeftImpl<>(t);
    }

    public static <T, U> Any2<T, U> right(@NonNull U u) {
        return new RightImpl(u);
    }

    public abstract Optional<T> getLeft();
    public abstract Optional<U> getRight();
    public abstract <X, Y> Any2<X, Y> map(Function<? super T, X> left_fn, Function<? super U, Y> right_fn);
    public abstract <X> X mapCombine(Function<? super T, ? extends X> left_fn, Function<? super U, ? extends X> right_fn);

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode(callSuper = false)
    private static final class LeftImpl<T, U> extends Any2<T, U> {
        @NonNull
        private final T v;

        @Override
        public Optional<T> getLeft() {
            return Optional.of(v);
        }

        @Override
        public Optional<U> getRight() {
            return Optional.empty();
        }

        @Override
        public <X, Y> Any2<X, Y> map(Function<? super T, X> left_fn, Function<? super U, Y> right_fn) {
            return Any2.left(left_fn.apply(v));
        }

        @Override
        public <X> X mapCombine(Function<? super T, ? extends X> left_fn, Function<? super U, ? extends X> right_fn) {
            return left_fn.apply(v);
        }
    };

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode(callSuper = false)
    private static final class RightImpl<T, U> extends Any2<T, U> {
        @NonNull
        private final U v;

        @Override
        public Optional<T> getLeft() {
            return Optional.empty();
        }

        @Override
        public Optional<U> getRight() {
            return Optional.of(v);
        }

        @Override
        public <X, Y> Any2<X, Y> map(Function<? super T, X> left_fn, Function<? super U, Y> right_fn) {
            return Any2.right(right_fn.apply(v));
        }

        @Override
        public <X> X mapCombine(Function<? super T, ? extends X> left_fn, Function<? super U, ? extends X> right_fn) {
            return right_fn.apply(v);
        }
    };
}
