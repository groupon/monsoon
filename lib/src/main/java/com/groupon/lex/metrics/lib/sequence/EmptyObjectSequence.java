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

import static com.groupon.lex.metrics.lib.sequence.ObjectSequence.reverseComparator;
import static java.util.Collections.emptyIterator;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EmptyObjectSequence<T> implements ObjectSequence<T> {
    @NonNull
    private final Comparator<?> comparator;

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public boolean isNonnull() {
        return true;
    }

    @Override
    public boolean isDistinct() {
        return true;
    }

    @Override
    public T get(int index) {
        throw new NoSuchElementException("index " + index + " out of bounds [0..0)");
    }

    @Override
    public <C extends Comparable<? super C>> Comparator<C> getComparator() {
        return (Comparator<C>) comparator;
    }

    @Override
    public Iterator<T> iterator() {
        return emptyIterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return new SpliteratorImpl<>(spliteratorCharacteristics(), comparator);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public Stream<T> parallelStream() {
        return StreamSupport.stream(spliterator(), true);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public ObjectSequence<T> reverse() {
        return new EmptyObjectSequence<>(reverseComparator(comparator));
    }

    @AllArgsConstructor
    private static class SpliteratorImpl<T> implements Spliterator<T> {
        private final int characteristics;
        private final Comparator<?> comparator;

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            return false;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return 0;
        }

        @Override
        public int characteristics() {
            return characteristics;
        }

        @Override
        public Comparator<? super T> getComparator() {
            if (!hasCharacteristics(Spliterator.SORTED))
                throw new IllegalStateException();
            return (Comparator<? super T>) comparator;
        }
    }
}
