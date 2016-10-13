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
import java.util.Spliterator;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

public interface Sequence extends Iterable<Integer> {
    @Override
    public Spliterator.OfInt spliterator();

    @Override
    public Iterator<Integer> iterator();

    public IntStream stream();

    public IntStream parallelStream();

    public Sequence reverse();

    public int size();

    /**
     * Yields the comparator that orders this sequence.
     *
     * Sequences are compared either in natural order or reverse order.
     *
     * @return The comparator comparing this sequence.
     */
    public <C extends Comparable<? super C>> Comparator<C> getComparator();

    public int get(int n);

    public Sequence skip(int n);

    public Sequence limit(int n);

    public default int[] toArray() {
        final int size = size();
        int result[] = new int[size];
        for (int i = 0; i < size; ++i)
            result[i] = get(i);
        return result;
    }

    public default boolean isEmpty() {
        return size() == 0;
    }

    public default <T> ObjectSequence<T> map(IntFunction<? extends T> fn, boolean sorted, boolean nonnull, boolean distinct) {
        return new IntObjectSequence<>(this, fn, sorted, nonnull, distinct);
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
    public default EqualRange equalRange(IntUnaryOperator cmpFn) {
        int begin = 0, end = size();
        final int inside;
        for (;;) {
            if (begin == end) {
                return new EqualRange(begin, end);
            }
            final int mid = (begin + end) / 2;
            int cmp = cmpFn.applyAsInt(get(mid));

            if (cmp < 0) {
                begin = mid + 1;
            } else if (cmp > 0) {
                end = mid;
            } else {
                inside = mid;
                break;
            }
        }

        int begin_end = inside;
        while (begin != begin_end) {
            final int mid = (begin + begin_end) / 2;
            int cmp = cmpFn.applyAsInt(get(mid));

            if (cmp < 0) {
                begin = mid + 1;
            } else {
                begin_end = mid;
            }
        }

        int end_begin = inside;
        while (end_begin != end) {
            final int mid = (end_begin + end) / 2;
            int cmp = cmpFn.applyAsInt(get(mid));

            if (cmp <= 0) {
                end_begin = mid + 1;
            } else {
                end = mid;
            }
        }

        return new EqualRange(begin, end);
    }
}
