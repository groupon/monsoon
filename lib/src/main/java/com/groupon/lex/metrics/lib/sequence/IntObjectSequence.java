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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class IntObjectSequence<T> implements ObjectSequence<T> {
    @NonNull
    private final Sequence underlying;
    @NonNull
    private final IntFunction<? extends T> fn;
    @Getter
    private final boolean sorted, nonnull, distinct;

    @Override
    public T get(int index) {
        if (index < 0 || index >= size())
            throw new NoSuchElementException("index " + index + " out of bounds [0.." + size() + ")");
        return fn.apply(underlying.get(index));
    }

    @Override
    public <C extends Comparable<? super C>> Comparator<C> getComparator() {
        return sorted ? underlying.getComparator() : null;
    }

    @Override
    public Iterator<T> iterator() {
        return new IteratorImpl<>(underlying.iterator(), fn);
    }

    @Override
    public Spliterator<T> spliterator() {
        return new SpliteratorImpl<>(underlying.spliterator(), fn, spliteratorCharacteristics());
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(this::spliterator, spliteratorCharacteristics(), false);
    }

    @Override
    public Stream<T> parallelStream() {
        return StreamSupport.stream(this::spliterator, spliteratorCharacteristics(), true);
    }

    @Override
    public IntObjectSequence<T> limit(int n) {
        if (n < 0 || n > size())
            throw new NoSuchElementException("index " + n + " outside range [0.." + size() + "]");
        if (n == size())
            return this;
        return new IntObjectSequence<>(underlying.limit(n), fn, sorted, nonnull, distinct);
    }

    @Override
    public IntObjectSequence<T> skip(int n) {
        if (n < 0 || n > size())
            throw new NoSuchElementException("index " + n + " outside range [0.." + size() + "]");
        if (n == 0)
            return this;
        return new IntObjectSequence<>(underlying.skip(n), fn, sorted, nonnull, distinct);
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
    public IntObjectSequence<T> reverse() {
        return new IntObjectSequence<>(underlying.reverse(), fn, sorted, nonnull, distinct);
    }

    @Override
    public <R> IntObjectSequence<R> map(Function<? super T, ? extends R> fn, boolean sorted, boolean nonnull, boolean distinct) {
        return new IntObjectSequence<>(underlying, (i) -> fn.apply(this.fn.apply(i)), sorted, nonnull, distinct);
    }

    @RequiredArgsConstructor
    private static class IteratorImpl<T> implements Iterator<T> {
        @NonNull
        private final Iterator<Integer> underlying;
        @NonNull
        private final IntFunction<? extends T> fn;

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public T next() {
            return fn.apply(underlying.next());
        }
    }

    @RequiredArgsConstructor
    private static class SpliteratorImpl<T> implements Spliterator<T> {
        @NonNull
        private final Spliterator.OfInt underlying;
        @NonNull
        private final IntFunction<? extends T> fn;
        private final int characteristics;

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            return underlying.tryAdvance(doAction(action));
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            underlying.forEachRemaining(doAction(action));
        }

        @Override
        public Spliterator<T> trySplit() {
            final Spliterator.OfInt underlyingSplit = underlying.trySplit();
            return (underlyingSplit == null ? null : new SpliteratorImpl(underlyingSplit, fn, characteristics));
        }

        @Override
        public long estimateSize() {
            return underlying.estimateSize();
        }

        @Override
        public int characteristics() {
            return characteristics;
        }

        private IntConsumer doAction(Consumer<? super T> action) {
            return (v) -> action.accept(fn.apply(v));
        }

        @Override
        public Comparator<? super T> getComparator() {
            if (!hasCharacteristics(Spliterator.SORTED))
                throw new IllegalStateException();
            return (Comparator<? super T>) underlying.getComparator();
        }
    }
}
