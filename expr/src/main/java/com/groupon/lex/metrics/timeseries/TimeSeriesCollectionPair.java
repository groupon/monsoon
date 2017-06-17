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
package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import java.util.ArrayList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.reverse;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public interface TimeSeriesCollectionPair {
    public default TimeSeriesCollection getCurrentCollection() {
        return getPreviousCollection(0)
                .orElseThrow(() -> new IllegalStateException("current collection may never be null"));
    }

    public default TimeSeriesCollection getPreviousCollection() {
        return getPreviousCollection(1)
                .orElseGet(() -> new EmptyTimeSeriesCollection(getCurrentCollection().getTimestamp()));
    }

    public Optional<TimeSeriesCollection> getPreviousCollection(int n);

    public default Optional<TimeSeriesCollection> getPreviousCollection(Duration duration) {
        final DateTime ts = getCurrentCollection().getTimestamp().minus(duration);

        for (int i = 0; i < size(); ++i) {
            final TimeSeriesCollection collection = getPreviousCollection(i).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist"));
            if (!collection.getTimestamp().isAfter(ts))
                return Optional.of(collection);
        }
        return Optional.empty();
    }

    public default TimeSeriesCollectionPair getPreviousCollectionPair(int n) {
        if (n < 0)
            throw new IllegalArgumentException("cannot look into the future");
        if (n == 0) return this;
        if (n >= size())
            return new ImmutableTimeSeriesCollectionPair(EMPTY_LIST);

        List<TimeSeriesCollection> tail = new ArrayList<>(size() - n);
        for (int i = n; i < size(); ++i)
            tail.add(getPreviousCollection(i).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist")));
        return new ImmutableTimeSeriesCollectionPair(tail, this);
    }

    public default TimeSeriesCollectionPair getPreviousCollectionPair(Duration duration) {
        final DateTime ts = getCurrentCollection().getTimestamp().minus(duration);
        if (!getCurrentCollection().getTimestamp().isAfter(ts)) return this;

        List<TimeSeriesCollection> tail = new ArrayList<>();
        int i;
        for (i = 1; i < size(); ++i) {
            final TimeSeriesCollection collection = getPreviousCollection(i).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist"));
            if (!collection.getTimestamp().isAfter(ts)) {
                tail.add(collection);
                ++i;
                break;
            }
        }

        for (int k = i; k < size(); ++k)
            tail.add(getPreviousCollection(k).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist")));
        return new ImmutableTimeSeriesCollectionPair(tail, this);
    }

    public default List<TimeSeriesCollectionPair> getCollectionPairsSince(Duration duration) {
        final DateTime ts = getCurrentCollection().getTimestamp().minus(duration);

        int lastIdx;
        for (lastIdx = 0; lastIdx < size(); ++lastIdx) {
            final TimeSeriesCollection collection = getPreviousCollection(lastIdx).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist"));
            if (collection.getTimestamp().isBefore(ts))
                break;
        }

        List<TimeSeriesCollectionPair> pairs = new ArrayList<>(lastIdx);
        for (int idx = lastIdx - 1; idx >= 0; --idx)
            pairs.add(getPreviousCollectionPair(idx));
        return pairs;
    }

    public static TimeSeriesCollection getPreviousCollectionAt(List<TimeSeriesCollection> list, DateTime ts) {
        for (int i = 0; i < list.size(); ++i) {
            final TimeSeriesCollection collection = list.get(i);

            if (collection.getTimestamp().equals(ts)) {
                List<TimeSeriesCollection> forward = list.subList(0, i);
                reverse(forward);
                List<TimeSeriesCollection> backward = list.subList(i + 1, list.size());
                return new InterpolatedTSC(collection, backward, forward);
            } else if (collection.getTimestamp().isBefore(ts)) {
                List<TimeSeriesCollection> forward = list.subList(0, i);
                reverse(forward);
                List<TimeSeriesCollection> backward = list.subList(i, list.size());
                return new InterpolatedTSC(new EmptyTimeSeriesCollection(ts), backward, forward);
            }
        }

        return new EmptyTimeSeriesCollection(ts);
    }

    public static TimeSeriesCollectionPair getPreviousCollectionPairAt(List<TimeSeriesCollection> list, DateTime ts, TimeSeriesCollectionPair parent) {
        TimeSeriesCollection interpolated = null;
        List<TimeSeriesCollection> backward = null;
        for (int i = 0; i < list.size(); ++i) {
            final TimeSeriesCollection collection = list.get(i);

            if (collection.getTimestamp().equals(ts)) {
                List<TimeSeriesCollection> forward = new ArrayList<>(list.subList(0, i));
                reverse(forward);
                backward = new ArrayList<>(list.subList(i + 1, list.size()));
                interpolated = new InterpolatedTSC(collection, backward, forward);
                break;
            } else if (collection.getTimestamp().isBefore(ts)) {
                List<TimeSeriesCollection> forward = new ArrayList<>(list.subList(0, i));
                reverse(forward);
                backward = new ArrayList<>(list.subList(i, list.size()));
                interpolated = new InterpolatedTSC(new EmptyTimeSeriesCollection(ts), backward, forward);
                break;
            }
        }
        if (interpolated == null)
            interpolated = new InterpolatedTSC(new EmptyTimeSeriesCollection(ts), EMPTY_LIST, EMPTY_LIST);
        if (backward == null) backward = new ArrayList<>(1);
        backward.add(0, interpolated);

        return new ImmutableTimeSeriesCollectionPair(backward, parent);
    }

    public default TimeSeriesCollection getPreviousCollectionAt(DateTime ts) {
        List<TimeSeriesCollection> list = new ArrayList<>();
        for (int i = 0; i < size(); ++i)
            list.add(getPreviousCollection(i).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist")));
        return getPreviousCollectionAt(list, ts);
    }

    public default TimeSeriesCollection getPreviousCollectionAt(Duration duration) {
        return getPreviousCollectionAt(getCurrentCollection().getTimestamp().minus(duration));
    }

    public default TimeSeriesCollectionPair getPreviousCollectionPairAt(DateTime ts) {
        List<TimeSeriesCollection> list = new ArrayList<>();
        for (int i = 0; i < size(); ++i)
            list.add(getPreviousCollection(i).orElseThrow(() -> new IllegalStateException("Collections within range 0..size() must exist")));
        return getPreviousCollectionPairAt(list, ts, this);
    }

    public default TimeSeriesCollectionPair getPreviousCollectionPairAt(Duration duration) {
        return getPreviousCollectionPairAt(getCurrentCollection().getTimestamp().minus(duration));
    }

    public default Duration getCollectionInterval() {
        return new Duration(getPreviousCollection().getTimestamp(), getCurrentCollection().getTimestamp());
    }

    public default TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return getCurrentCollection().getTSValue(name);
    }

    public default Optional<TimeSeriesValueSet> getTSDeltaByName(GroupName name) {
        return getCurrentCollection().getTSDeltaByName(name);
    }

    public int size();
}
