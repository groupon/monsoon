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
package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.timeseries.BackRefTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.InterpolatedTSC;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class IntervalIterator implements Iterator<TimeSeriesCollection> {
    /** Iterator supplying items. */
    private final Iterator<TimeSeriesCollection> underlying;
    /** Window duration. */
    private final Duration lookBack, lookForward;
    /** Past window items in reverse chronological order. */
    private final Deque<TimeSeriesCollection> past = new ArrayDeque<>();
    /** Future window items in chronological order. */
    private final Deque<TimeSeriesCollection> future = new ArrayDeque<>();
    /** Look ahead item from underlying iterator. */
    private TimeSeriesCollection underlyingNext;
    /** Timestamp for next collection. */
    private DateTime nextTS;
    /** Interval which the iterator returns. */
    private final Duration interval;

    public static Stream<TimeSeriesCollection> stream(Iterator<TimeSeriesCollection> in, @NonNull Duration interval, Duration lookBack, Duration lookForward) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new IntervalIterator(in, interval, lookBack, lookForward),
                        NONNULL | IMMUTABLE | ORDERED | SORTED | DISTINCT),
                false);
    }

    public static Stream<TimeSeriesCollection> stream(Stream<TimeSeriesCollection> in, @NonNull Duration interval, Duration lookBack, Duration lookForward) {
        return stream(in.iterator(), interval, lookBack, lookForward);
    }

    public IntervalIterator(@NonNull Iterator<TimeSeriesCollection> underlying, @NonNull Duration interval, @NonNull Duration lookBack, @NonNull Duration lookForward) {
        this.interval = interval;
        this.underlying = underlying;
        this.lookBack = lookBack;
        this.lookForward = lookForward;

        consumeUnderlyingNext();  // Initialize underlyingNext.
        if (underlyingNext != null)
            nextTS = underlyingNext.getTimestamp();  // Initalize next timestamp.
    }

    @Override
    public boolean hasNext() {
        updateWindowBoundaries();
        return !future.isEmpty() || underlyingNext != null;
    }

    @Override
    public TimeSeriesCollection next() {
        return interpolated(updateWindow(), past, future);
    }

    private void updatePastAndFuture() {
        final DateTime tsBegin = nextTS.minus(lookBack);
        final DateTime tsEnd = nextTS.plus(lookForward);

        // Remove old entries outside the lookBack window.
        while (!past.isEmpty() && past.getLast().getTimestamp().isBefore(tsBegin))
            past.removeLast();

        // Move everything in future into past, that has a timestamp before nextTS.
        while (!future.isEmpty() && future.getFirst().getTimestamp().isBefore(nextTS)) {
            final TimeSeriesCollection f0 = future.removeFirst();
            if (!f0.getTimestamp().isBefore(tsBegin))
                past.addFirst(f0);
        }

        // Move underlying into past, until we reach nextTS.
        while (underlyingNext != null && underlyingNext.getTimestamp().isBefore(nextTS)) {
            if (!underlyingNext.getTimestamp().isBefore(tsBegin))
                past.addFirst(underlyingNext);
            consumeUnderlyingNext();
        }

        // Add items to future set until underlyingNext exceeds forward window.
        while (underlyingNext != null && !underlyingNext.getTimestamp().isAfter(tsEnd)) {
            future.addLast(underlyingNext);
            consumeUnderlyingNext();
        }
    }

    private void updateWindowBoundaries() {
        updatePastAndFuture();

        if (underlyingNext != null && future.isEmpty()) {
            // Skip forward until new data points are available.
            nextTS = underlyingNext.getTimestamp();
            updatePastAndFuture();
        } else if (!future.isEmpty() && !future.getFirst().getTimestamp().equals(nextTS)) {
            // Skip forward to next datapoint inside the window.
            nextTS = future.getFirst().getTimestamp();
            updatePastAndFuture();
        }
    }

    private TimeSeriesCollection updateWindow() {
        updateWindowBoundaries();

        // Only return something if we can.
        if (future.isEmpty() && underlyingNext == null)
            throw new NoSuchElementException();

        // Use head of future if timestamp matches, else synthesize empty collection.
        final TimeSeriesCollection next;
        if (future.getFirst().getTimestamp().equals(nextTS))
            next = future.removeFirst();
        else
            next = new BackRefTimeSeriesCollection(nextTS);

        nextTS = nextTS.plus(interval);

        return next;
    }

    private void consumeUnderlyingNext() {
        if (underlying.hasNext())
            underlyingNext = underlying.next();
        else
            underlyingNext = null;
    }

    private static TimeSeriesCollection interpolated(TimeSeriesCollection present, Collection<TimeSeriesCollection> past, Collection<TimeSeriesCollection> future) {
        if (future.isEmpty() || past.isEmpty())
            return present;  // Can't interpolate anyway.

        return new InterpolatedTSC(present, past, future);
    }
}
