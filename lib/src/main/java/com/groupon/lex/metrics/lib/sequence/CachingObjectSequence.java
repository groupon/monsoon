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

import java.lang.ref.Reference;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 *
 * @author ariane
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CachingObjectSequence<T> implements ObjectSequence<T> {
    private final ObjectSequence<T> underlying;
    private final Function<T, ? extends Reference<T>> referenceBuilder;
    private final CachedElement<T> cache[];

    public CachingObjectSequence(@NonNull ObjectSequence<T> underlying, @NonNull Function<T, ? extends Reference<T>> referenceBuilder) {
        this.underlying = underlying;
        this.referenceBuilder = referenceBuilder;
        this.cache = new CachedElement[underlying.size()];
        for (int i = 0; i < this.cache.length; ++i)
            cache[i] = new CachedElement<>();
    }

    @Override
    public boolean isSorted() {
        return underlying.isSorted();
    }

    @Override
    public boolean isNonnull() {
        return underlying.isNonnull();
    }

    @Override
    public boolean isDistinct() {
        return underlying.isDistinct();
    }

    @Override
    public T get(int index) {
        if (index < 0 || index >= size())
            throw new NoSuchElementException("index " + index + " out of bounds [0.." + size() + ")");
        return cache[index].resolve(() -> underlying.get(index), referenceBuilder);
    }

    @Override
    public <C extends Comparable<? super C>> Comparator<C> getComparator() {
        return underlying.getComparator();
    }

    @Override
    public Iterator<T> iterator() {
        return new ForwardSequence(0, size())
                .map(this::get, isSorted(), isNonnull(), isDistinct())
                .iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return new SpliteratorImpl();
    }

    @Override
    public Stream<T> stream() {
        return new ForwardSequence(0, size())
                .map(this::get, isSorted(), isNonnull(), isDistinct())
                .stream();
    }

    @Override
    public Stream<T> parallelStream() {
        return new ForwardSequence(0, size())
                .map(this::get, isSorted(), isNonnull(), isDistinct())
                .parallelStream();
    }

    @Override
    public int size() {
        return cache.length;
    }

    @Override
    public boolean isEmpty() {
        return cache.length == 0;
    }

    @Override
    public ObjectSequence<T> reverse() {
        CachedElement<T>[] newCache = new CachedElement[this.cache.length];
        for (int i = 0; i < newCache.length; ++i)
            newCache[newCache.length - 1 - i] = this.cache[i];
        return new CachingObjectSequence<>(underlying.reverse(), referenceBuilder, newCache);
    }

    private static class CachedElement<T> {
        private Reference<T> reference;

        public synchronized T resolve(Supplier<? extends T> supplier, Function<T, ? extends Reference<T>> referenceBuilder) {
            T result;
            if (reference != null) {
                result = reference.get();
                if (result != null)
                    return result;
            }

            result = supplier.get();
            if (result != null)
                reference = referenceBuilder.apply(result);
            return result;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    private class SpliteratorImpl implements Spliterator<T> {
        private int index = 0;
        private int end = cache.length;

        public SpliteratorImpl() {
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (index == end)
                return false;
            action.accept(get(index++));
            return true;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            while (index < end)
                action.accept(get(index++));
        }

        @Override
        public Spliterator<T> trySplit() {
            if (end - index < 2)
                return null;
            final int splitLen = (end - index) / 2;

            SpliteratorImpl result = new SpliteratorImpl(index, index + splitLen);
            index += splitLen;
            return result;
        }

        @Override
        public long estimateSize() {
            return end - index;
        }

        @Override
        public int characteristics() {
            return spliteratorCharacteristics();
        }

        @Override
        public Comparator<? super T> getComparator() {
            if (!hasCharacteristics(Spliterator.SORTED))
                throw new IllegalStateException();
            return (Comparator<? super T>) CachingObjectSequence.this.getComparator();
        }
    }
}
