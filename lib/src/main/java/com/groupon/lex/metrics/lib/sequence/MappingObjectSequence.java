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
package com.groupon.lex.metrics.lib.sequence;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;

/**
 *
 * @author ariane
 */
@AllArgsConstructor
public class MappingObjectSequence<T, R> implements ObjectSequence<R> {
    private final ObjectSequence<T> underlying;
    private final Function<? super T, ? extends R> fn;
    boolean sorted, nonnull, distinct;

    @Override
    public boolean isSorted() {
        return sorted && underlying.isSorted();
    }

    @Override
    public boolean isNonnull() {
        return nonnull && underlying.isNonnull();
    }

    @Override
    public boolean isDistinct() {
        return distinct && underlying.isDistinct();
    }

    @Override
    public R get(int index) {
        return fn.apply(underlying.get(index));
    }

    @Override
    public Iterator<R> iterator() {
        return new IteratorImpl<>(underlying.iterator(), fn);
    }

    @Override
    public Spliterator<R> spliterator() {
        return new SpliteratorImpl<>(underlying.spliterator(), fn, spliteratorCharacteristics());
    }

    @Override
    public Stream<R> stream() {
        return StreamSupport.stream(this::spliterator, spliteratorCharacteristics(), false);
    }

    @Override
    public Stream<R> parallelStream() {
        return StreamSupport.stream(this::spliterator, spliteratorCharacteristics(), true);
    }

    @Override
    public MappingObjectSequence<T, R> limit(int n) {
        if (n < 0 || n > size()) throw new NoSuchElementException("index " + n + " outside range [0.." + size() + "]");
        if (n == size()) return this;
        return new MappingObjectSequence<>(underlying.limit(n), fn, sorted, nonnull, distinct);
    }

    @Override
    public MappingObjectSequence<T, R> skip(int n) {
        if (n < 0 || n > size()) throw new NoSuchElementException("index " + n + " outside range [0.." + size() + "]");
        if (n == 0) return this;
        return new MappingObjectSequence<>(underlying.skip(n), fn, sorted, nonnull, distinct);
    }

    @Override
    public int size() {
        return underlying.size();
    }

    @Override
    public boolean isEmpty() {
        return underlying.isEmpty();
    }

    @Override
    public ObjectSequence<R> reverse() {
        return new MappingObjectSequence<>(underlying.reverse(), fn, sorted, nonnull, distinct);
    }

    @Override
    public <U> ObjectSequence<U> map(Function<? super R, ? extends U> fn, boolean sorted, boolean nonnull, boolean distinct) {
        return new MappingObjectSequence<>(underlying, (v) -> fn.apply(this.fn.apply(v)), sorted, nonnull, distinct);
    }

    @AllArgsConstructor
    private static class IteratorImpl<T, R> implements Iterator<R> {
        private final Iterator<T> underlying;
        private final Function<? super T, ? extends R> fn;

        @Override
        public boolean hasNext() { return underlying.hasNext(); }
        @Override
        public R next() { return fn.apply(underlying.next()); }
    }

    @AllArgsConstructor
    private static class SpliteratorImpl<T, R> implements Spliterator<R> {
        private final Spliterator<T> underlying;
        private final Function<? super T, ? extends R> fn;
        private final int characteristics;

        @Override
        public boolean tryAdvance(Consumer<? super R> action) {
            return underlying.tryAdvance((v) -> action.accept(fn.apply(v)));
        }

        @Override
        public void forEachRemaining(Consumer<? super R> action) {
            underlying.forEachRemaining((v) -> action.accept(fn.apply(v)));
        }

        @Override
        public Spliterator<R> trySplit() {
            Spliterator<T> underlyingSplit = underlying.trySplit();
            if (underlyingSplit == null) return null;
            return new SpliteratorImpl<>(underlyingSplit, fn, characteristics);
        }

        @Override
        public long estimateSize() {
            return underlying.estimateSize();
        }

        @Override
        public int characteristics() {
            return characteristics;
        }
    }
}
