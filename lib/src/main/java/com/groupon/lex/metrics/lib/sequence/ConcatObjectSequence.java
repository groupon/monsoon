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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;

/**
 *
 * @author ariane
 */
public class ConcatObjectSequence<T> implements ObjectSequence<T> {
    private final ObjectSequence<T> list[];
    private final int endOffsetList[];  // Contains offset at end of corresponding list entry.
    @Getter
    private final boolean sorted, nonnull, distinct;
    private final Comparator<?> comparator;

    public ConcatObjectSequence(ObjectSequence<T> list[], boolean sorted, boolean distinct) {
        this.list = Arrays.copyOf(list, list.length);
        this.comparator = matchingComparator(list);
        this.sorted = sorted && this.comparator != null && Arrays.stream(this.list).allMatch(ObjectSequence::isSorted);
        this.nonnull = Arrays.stream(this.list).allMatch(ObjectSequence::isNonnull);
        this.distinct = distinct && Arrays.stream(this.list).allMatch(ObjectSequence::isDistinct);

        this.endOffsetList = new int[this.list.length];
        int accumulator = 0;
        for (int i = 0; i < this.list.length; ++i) {
            accumulator += list[i].size();
            endOffsetList[i] = accumulator;
        }
    }

    public ConcatObjectSequence(Collection<ObjectSequence<T>> list, boolean sorted, boolean distinct) {
        this(list.toArray(new ObjectSequence[list.size()]), sorted, distinct);
    }

    private static Comparator<?> matchingComparator(ObjectSequence<?> list[]) {
        if (list.length == 0)
            return Comparator.naturalOrder();
        Comparator<?> c = list[0].getComparator();
        for (int i = 1; i < list.length; ++i)
            if (c != list[i].getComparator())
                return null;
        return c;
    }

    @Override
    public T get(int index) throws NoSuchElementException {
        final int subIndex = new ForwardSequence(0, list.length)
                .map(idx -> endOffsetList[idx], true, true, false)
                .equalRange((offset) -> Integer.compare(offset - 1, index))
                .getBegin();
        if (subIndex < 0 || subIndex >= list.length)
            throw new NoSuchElementException("index " + index + " outside range [0.." + size() + "]");
        return list[subIndex].get(index - (subIndex == 0 ? 0 : endOffsetList[subIndex - 1]));
    }

    @Override
    public <C extends Comparable<? super C>> Comparator<C> getComparator() {
        return (Comparator<C>) comparator;
    }

    @Override
    public Iterator<T> iterator() {
        return new IteratorImpl<>(list);
    }

    @Override
    public Spliterator<T> spliterator() {
        return new SpliteratorImpl<>(list, spliteratorCharacteristics(), getComparator());
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
    public int size() {
        if (endOffsetList.length == 0)
            return 0;
        return endOffsetList[endOffsetList.length - 1];
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public ConcatObjectSequence<T> reverse() {
        ObjectSequence<T> copyList[] = new ObjectSequence[list.length];
        for (int i = 0; i < list.length; ++i)
            copyList[copyList.length - 1 - i] = list[i].reverse();
        return new ConcatObjectSequence<>(copyList, sorted, distinct);
    }

    private static class IteratorImpl<T> implements Iterator<T> {
        private final Iterator<T> iters[];
        private int index;

        public IteratorImpl(ObjectSequence<T> list[]) {
            this.iters = new Iterator[list.length];
            this.index = 0;
            for (int i = 0; i < list.length; ++i)
                iters[i] = list[i].iterator();
        }

        @Override
        public boolean hasNext() {
            if (index == iters.length) {
                return false;
            }
            while (!iters[index].hasNext()) {
                iters[index++] = null;  // Release resources.
                if (index == iters.length)
                    return false;
            }

            return iters[index].hasNext();
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException("iterator end");
            return iters[index].next();
        }
    }

    private static class SpliteratorImpl<T> implements Spliterator<T> {
        private final Spliterator<T> spliters[];
        private int index = 0;
        private final int characteristics;
        private final Comparator<?> comparator;

        public SpliteratorImpl(ObjectSequence<T> list[], int characteristics, Comparator<?> comparator) {
            this.spliters = new Spliterator[list.length];
            this.characteristics = characteristics;
            for (int i = 0; i < list.length; ++i)
                spliters[i] = list[i].spliterator();
            this.comparator = comparator;
        }

        private SpliteratorImpl(Spliterator<T> spliters[], int characteristics, Comparator<?> comparator) {
            this.spliters = spliters;
            this.characteristics = characteristics;
            this.comparator = comparator;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (index == spliters.length)
                return false;
            while (!spliters[index].tryAdvance(action)) {
                spliters[index++] = null;  // Release resources.
                if (index == spliters.length)
                    return false;
            }
            return true;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            while (index < spliters.length) {
                spliters[index].forEachRemaining(action);
                spliters[index++] = null;  // Release resources.
            }
        }

        @Override
        public Spliterator<T> trySplit() {
            final int splitLen;
            if (spliters.length - index < 2)
                return null;
            else
                splitLen = (spliters.length - index) / 2;

            if (splitLen == 1) {
                Spliterator<T> result = spliters[index];
                spliters[index++] = null;  // Release resources.
                return result;
            }

            Spliterator<T> splittedList[] = Arrays.copyOfRange(spliters, index, index + splitLen);
            Arrays.fill(spliters, index, index + splitLen, null);  // Release resources.
            index += splitLen;
            return new SpliteratorImpl<>(splittedList, characteristics, comparator);
        }

        @Override
        public long estimateSize() {
            long sum = 0;
            for (int i = index; i < spliters.length; ++i)
                sum += spliters[i].estimateSize();
            return sum;
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
