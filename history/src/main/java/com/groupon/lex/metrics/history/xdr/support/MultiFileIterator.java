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

import com.groupon.lex.metrics.timeseries.BackRefTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.stream.Stream;
import lombok.NonNull;
import org.joda.time.DateTime;

/**
 * Iterator that traverses multiple TSData sources.
 *
 * The iterator will take care of emitting elements in order and merging duplicate
 * entries together.
 * @author ariane
 */
public class MultiFileIterator implements Iterator<TimeSeriesCollection> {
    private final Comparator<DateTime> cmp;
    private final PriorityQueue<TSDataSupplier> files;
    private final PriorityQueue<PeekableIterator> fileIters;

    public MultiFileIterator(@NonNull Collection<? extends TSDataSupplier> files, @NonNull Comparator<DateTime> cmp) {
        this.cmp = cmp;
        this.files = new PriorityQueue<>(Comparator.comparing(TSDataSupplier::getBegin, this.cmp));
        this.fileIters = new PriorityQueue<>(Comparator.comparing(iter -> iter.peek().getTimestamp(), this.cmp));

        this.files.addAll(files);
        updateFileIters();
    }

    @Override
    public boolean hasNext() {
        return !fileIters.isEmpty();
    }

    @Override
    public TimeSeriesCollection next() {
        return getNextCollection();
    }

    private TimeSeriesCollection getNextCollection() {
        final PeekableIterator f0 = fileIters.poll();
        if (f0 == null) throw new NoSuchElementException();
        TimeSeriesCollection next = f0.next();
        if (f0.hasNext())
            fileIters.add(f0);  // Re-insert at new position.

        List<TimeSeriesCollection> tsMerge = new ArrayList<>();
        for (;;) {
            final PeekableIterator fn = fileIters.peek();
            if (fn == null) break;
            if (cmp.compare(next.getTimestamp(), fn.peek().getTimestamp()) != 0) break;
            final PeekableIterator removed = fileIters.poll();
            assert(fn == removed);

            tsMerge.add(fn.next());
            if (fn.hasNext())
                fileIters.add(fn);  // Re-insert at new position.
        }

        /*
         * If multiple TS Collections share the same timestamp, merge them together.
         */
        if (!tsMerge.isEmpty()) {
            next = new BackRefTimeSeriesCollection(
                    next.getTimestamp(),
                    Stream.concat(next.getTSValues().stream(), tsMerge.stream().flatMap(c -> c.getTSValues().stream())));
        }

        updateFileIters();  // Update file iters after using them.
        return next;
    }

    private void updateFileIters() {
        // Ensure at least 1 iterator is present in fileIters.
        while (fileIters.isEmpty()) {
            final TSDataSupplier f0 = files.poll();
            if (f0 == null)
                return;  // No more iterators to create.
            final Iterator<TimeSeriesCollection> f0_iter = f0.getIterator();
            if (f0_iter.hasNext())
                fileIters.add(new PeekableIterator(f0_iter));
        }

        // Add all iterators that start at/before current element of fileIters head.
        final DateTime ts = fileIters.peek().peek().getTimestamp();
        for (;;) {
            final TSDataSupplier f0 = files.peek();
            if (f0 == null) break;
            if (cmp.compare(ts, f0.getBegin()) < 0) break;
            final TSDataSupplier removed = files.poll(); // Use f0.
            assert(f0 == removed);

            final Iterator<TimeSeriesCollection> newIter = f0.getIterator();
            if (newIter.hasNext())
                fileIters.add(new PeekableIterator(newIter));
        }
    }

    /**
     * Interface to be implemented for files handled by this iterator.
     */
    public static interface TSDataSupplier {
        /** Get the begin timestamp of this file. */
        public DateTime getBegin();
        /**
         * Get an iterator iterating this file.
         *
         * The iterator must traverse elements in chronological order.
         * Each returned element must have a timestamp at/after getBegin().
         * @return An iterator over the collections in the current file.
         */
        public Iterator<TimeSeriesCollection> getIterator();
    }

    /**
     * Simple iterator wrapper that allows us to look at the next element without
     * consuming it.
     */
    private static class PeekableIterator implements Iterator<TimeSeriesCollection> {
        private final Iterator<TimeSeriesCollection> iter;
        private TimeSeriesCollection next;

        public PeekableIterator(@NonNull Iterator<TimeSeriesCollection> iter) {
            this.iter = iter;
            if (this.iter.hasNext())
                next = this.iter.next();
            else
                next = null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public TimeSeriesCollection next() {
            final TimeSeriesCollection result = next;
            if (result == null)
                throw new NoSuchElementException();
            if (iter.hasNext())
                next = iter.next();
            else
                next = null;
            return result;
        }

        public TimeSeriesCollection peek() {
            if (next == null) throw new NoSuchElementException();
            return next;
        }
    }
}
