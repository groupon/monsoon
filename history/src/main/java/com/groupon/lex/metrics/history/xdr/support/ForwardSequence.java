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
package com.groupon.lex.metrics.history.xdr.support;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * A bounded sequence.
 */
@Value
public class ForwardSequence implements Sequence {
    public static int SPLITERATOR_CHARACTERISTICS = Spliterator.ORDERED |
            Spliterator.SORTED |
            Spliterator.SIZED |
            Spliterator.SUBSIZED |
            Spliterator.DISTINCT |
            Spliterator.IMMUTABLE |
            Spliterator.NONNULL;

    private final int begin, end;

    @Override
    public Spliterator.OfInt spliterator() {
        return new SpliteratorImpl(begin, end);
    }

    @Override
    public Iterator<Integer> iterator() {
        return new IteratorImpl(begin, end);
    }

    @Override
    public IntStream stream() {
        return StreamSupport.intStream(spliterator(), false);
    }

    @Override
    public IntStream parallelStream() {
        return StreamSupport.intStream(spliterator(), true);
    }

    @Override
    public Sequence reverse() {
        return new ReverseSequence(begin, end);
    }

    @Override
    public int size() {
        return Integer.max(begin, end) - begin;
    }

    @AllArgsConstructor
    private static class IteratorImpl implements Iterator<Integer> {
        private int begin;
        private final int end;

        @Override
        public boolean hasNext() { return begin < end; }
        @Override
        public Integer next() {
            if (begin >= end) throw new NoSuchElementException();
            return begin++;
        }
    }

    @AllArgsConstructor
    private static class SpliteratorImpl implements Spliterator.OfInt {
        private int begin;
        private final int end;

        @Override
        public boolean tryAdvance(IntConsumer action) {
            if (begin >= end) return false;
            action.accept(begin++);
            return true;
        }

        @Override
        public void forEachRemaining(IntConsumer action) {
            for (int i = begin; i < end; ++i) action.accept(i);
            begin = end;
        }

        @Override
        public Spliterator.OfInt trySplit() {
            if (begin >= end || end - begin < 2) return null;

            final int split = begin + (end - begin) / 2;
            final int rvBegin = begin;
            begin = split;
            return new SpliteratorImpl(rvBegin, split);
        }

        @Override
        public long estimateSize() {
            return Integer.max(begin, end) - begin;
        }

        @Override
        public int characteristics() {
            return SPLITERATOR_CHARACTERISTICS;
        }

        @Override
        public Comparator<Integer> getComparator() { return null; }
    };
}
