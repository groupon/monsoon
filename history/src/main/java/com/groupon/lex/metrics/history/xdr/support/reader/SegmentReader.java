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
package com.groupon.lex.metrics.history.xdr.support.reader;

import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;

public interface SegmentReader<T> {
    public abstract T decode() throws IOException, OncRpcException;

    public default <Exc extends Exception> T decodeOrThrow(Function<Exception, Exc> exc) throws Exc {
        try {
            return decode();
        } catch (IOException | OncRpcException ex) {
            throw exc.apply(ex);
        }
    }

    public default T decodeOrThrow() throws DecodingException {
        return decodeOrThrow((cause) -> new DecodingException("segment decode failed", cause));
    }

    public static <T> SegmentReader<T> of(T v) {
        return () -> v;
    }

    public static <T> SegmentReader<T> ofSupplier(Supplier<T> v) {
        return () -> v.get();
    }

    public default <U> SegmentReader<U> map(Function<? super T, ? extends U> fn) {
        return new MappedSegmentReader<>(this, fn);
    }

    public default <U> SegmentReader<U> flatMap(Function<? super T, ? extends SegmentReader<? extends U>> fn) {
        return () -> fn.apply(this.decode()).decode();
    }

    public default <U, R> SegmentReader<R> combine(SegmentReader<U> other, BiFunction<? super T, ? super U, ? extends R> fn) {
        return () -> fn.apply(this.decode(), other.decode());
    }

    public default SegmentReader<T> peek(Consumer<? super T> fn) {
        return new PeekedSegmentReader<>(this, fn);
    }

    public default SegmentReader<T> share() {
        return new WeakSharedSegmentReader<>(this);
    }

    public default SegmentReader<T> cache() {
        return new SoftSharedSegmentReader<>(this);
    }

    public default SegmentReader<T> cache(T current) {
        return new SoftSharedSegmentReader<>(this, current);
    }

    public default SegmentReader<Optional<T>> filter(Predicate<? super T> pred) {
        return new FilterSegmentReader<>(this, pred);
    }

    public static interface Factory<BaseType> {
        public <T extends BaseType> SegmentReader<T> get(Supplier<T> type, FilePos pos);
    }
}

@RequiredArgsConstructor
class MappedSegmentReader<T, U> implements SegmentReader<U> {
    private final SegmentReader<T> in;
    private final Function<? super T, ? extends U> fn;

    @Override
    public U decode() throws IOException, OncRpcException {
        return fn.apply(in.decode());
    }
}

@RequiredArgsConstructor
class PeekedSegmentReader<T> implements SegmentReader<T> {
    private final SegmentReader<T> in;
    private final Consumer<? super T> fn;

    @Override
    public T decode() throws IOException, OncRpcException {
        T v = in.decode();
        fn.accept(v);
        return v;
    }
}

@RequiredArgsConstructor
class WeakSharedSegmentReader<T> implements SegmentReader<T> {
    private final SegmentReader<T> in;
    private Reference<T> ref = new WeakReference<>(null);

    @Override
    public synchronized T decode() throws IOException, OncRpcException {
        T v = ref.get();
        if (v != null) return v;

        v = in.decode();
        ref = new WeakReference<>(v);
        return v;
    }
}

@RequiredArgsConstructor
class SoftSharedSegmentReader<T> implements SegmentReader<T> {
    private final SegmentReader<T> in;
    private Reference<T> ref = new SoftReference<>(null);

    public SoftSharedSegmentReader(SegmentReader<T> in, T current) {
        this(in);
        ref = new SoftReference(current);
    }

    @Override
    public synchronized T decode() throws IOException, OncRpcException {
        T v = ref.get();
        if (v != null) return v;

        v = in.decode();
        ref = new SoftReference<>(v);
        return v;
    }
}

@RequiredArgsConstructor
class FilterSegmentReader<T> implements SegmentReader<Optional<T>> {
    private final SegmentReader<T> in;
    private final Predicate<? super T> pred;

    @Override
    public Optional<T> decode() throws IOException, OncRpcException {
        return Optional.of(in.decode())
                .filter(pred);
    }
}