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

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public interface ObjectSequence<T> extends Iterable<T> {
    public boolean isSorted();

    public boolean isNonnull();

    public boolean isDistinct();

    public T get(int index) throws NoSuchElementException;

    public Iterator<T> iterator();

    public Spliterator<T> spliterator();

    public Stream<T> stream();

    public Stream<T> parallelStream();

    public int size();

    public boolean isEmpty();

    public ObjectSequence<T> reverse();

    public default T first() throws NoSuchElementException {
        return get(0);
    }

    public default T last() throws NoSuchElementException {
        return get(size() - 1);
    }

    public static <T> ObjectSequence<T> empty() {
        return new EmptyObjectSequence<>();
    }

    public default <R> ObjectSequence<R> map(Function<? super T, ? extends R> fn, boolean sorted, boolean nonnull, boolean distinct) {
        return new MappingObjectSequence<>(this, fn, sorted, nonnull, distinct);
    }

    public default ObjectSequence<T> peek(Consumer<? super T> fn) {
        return map(value -> {
            fn.accept(value);
            return value;
        }, true, true, true);
    }

    public default Object[] toArray() {
        return stream().toArray();
    }

    public default <R> R[] toArray(R[] a) {
        return stream().toArray((size) -> {
            return (R[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        });
    }

    public default ObjectSequence<T> share() {
        return new CachingObjectSequence<>(this, (v) -> new WeakReference<>(v));
    }

    public default ObjectSequence<T> cache() {
        return new CachingObjectSequence<>(this, (v) -> new SoftReference<>(v));
    }

    public default ObjectSequence<T> limit(int n) {
        if (n < 0 || n > size()) {
            throw new NoSuchElementException("index " + n + " outside range [0.." + size() + "]");
        }
        if (n == 0) {
            return empty();
        }
        if (n == size()) {
            return this;
        }
        return new ForwardSequence(0, n)
                .map(this::get, isSorted(), isNonnull(), isDistinct());
    }

    public default ObjectSequence<T> skip(int n) {
        if (n < 0 || n > size()) {
            throw new NoSuchElementException("index " + n + " outside range [0.." + size() + "]");
        }
        if (n == 0) {
            return this;
        }
        if (n == size()) {
            return empty();
        }
        return new ForwardSequence(n, size())
                .map(this::get, isSorted(), isNonnull(), isDistinct());
    }

    public default int spliteratorCharacteristics() {
        return Spliterator.ORDERED
                | Spliterator.SIZED
                | Spliterator.SUBSIZED
                | Spliterator.IMMUTABLE
                | (isSorted() ? Spliterator.SORTED : 0)
                | (isDistinct() ? Spliterator.DISTINCT : 0)
                | (isNonnull() ? Spliterator.NONNULL : 0);
    }

    public static <T> ObjectSequence<T> concat(ObjectSequence<T> seq[], boolean sorted, boolean unique) {
        return new ConcatObjectSequence<>(seq, sorted, unique);
    }

    public static <T> ObjectSequence<T> concat(Collection<ObjectSequence<T>> seq, boolean sorted, boolean unique) {
        return new ConcatObjectSequence<>(seq, sorted, unique);
    }

    public static <T> ObjectSequence<T> of(boolean sorted, boolean nonnull, boolean distinct, T... values) {
        return new ForwardSequence(0, values.length)
                .map(idx -> values[idx], sorted, nonnull, distinct);
    }

    /**
     * Find the range of items that are equal according to the given predicate.
     *
     * @param cmpFn A function that return 0 if the argument is equal. Less than
     * zero if the argument is before the sought range. More than zero if the
     * argument is after the sought range.
     * @return The indices in the sequence, such that 'begin' is the first
     * matching element and 'end' is the first non-matching element. If no
     * element matches, 'begin' = 'end' and 'begin' points at the index where
     * the element should be, if it were present.
     */
    public default EqualRange equalRange(ToIntFunction<? super T> cmpFn) {
        return new ForwardSequence(0, size()).equalRange((idx) -> cmpFn.applyAsInt(get(idx)));
    }
}
