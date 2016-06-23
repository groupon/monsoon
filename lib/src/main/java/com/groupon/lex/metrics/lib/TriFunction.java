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

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 * @author ariane
 */
public interface TriFunction<X, Y, Z, R> {
    public R apply(X x, Y y, Z z);

    public default BiFunction<Y, Z, R> bind1(X x) {
        return (Y y, Z z) -> apply(x, y, z);
    }

    public default BiFunction<X, Z, R> bind2(Y y) {
        return (X x, Z z) -> apply(x, y, z);
    }

    public default BiFunction<X, Y, R> bind3(Z z) {
        return (X x, Y y) -> apply(x, y, z);
    }

    public default <XAlt> TriFunction<XAlt, Y, Z, R> bind1_fn(Function<? super XAlt, ? extends X> fn) {
        return (XAlt x, Y y, Z z) -> apply(fn.apply(x), y, z);
    }

    public default <YAlt> TriFunction<X, YAlt, Z, R> bind2_fn(Function<? super YAlt, ? extends Y> fn) {
        return (X x, YAlt y, Z z) -> apply(x, fn.apply(y), z);
    }

    public default <ZAlt> TriFunction<X, Y, ZAlt, R> bind3_fn(Function<? super ZAlt, ? extends Z> fn) {
        return (X x, Y y, ZAlt z) -> apply(x, y, fn.apply(z));
    }

    public default <V> TriFunction<X, Y, Z, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (X x, Y y, Z z) -> after.apply(apply(x, y, z));
    }
}
