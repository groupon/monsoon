/*
 * Copyright (c) 2017, ariane
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.groupon.monsoon.history.influx;

import static com.github.groupon.monsoon.history.influx.InfluxUtil.extractTimestamps;
import static com.github.groupon.monsoon.history.influx.InfluxUtil.isSorted;
import static com.github.groupon.monsoon.history.influx.InfluxUtil.throwOnResultError;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.NonNull;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class ForwardStream implements Spliterator<TimeSeriesCollection> {
    public static final int SPLITERATOR_CHARACTERISTICS = Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.CONCURRENT | Spliterator.SORTED;
    private static final Duration MIN_INTERVAL = Duration.standardHours(1);
    private final SelectStatement selector;
    private DateTime begin;
    private final DateTime end;
    private Deque<TimeSeriesCollection> pending = null;

    private ForwardStream(@NonNull SelectStatement selector, @NonNull DateTime begin, @NonNull DateTime end) {
        this.selector = selector;
        this.begin = begin;
        this.end = end;
    }

    public ForwardStream(InfluxDB influxDB, String database, DateTime begin, DateTime end, TimeSeriesMetricFilter filter) {
        this(new SelectStatement(influxDB, database, filter), begin, end);
    }

    @Override
    public boolean tryAdvance(Consumer<? super TimeSeriesCollection> action) {
        update();
        TimeSeriesCollection head = pending.pollFirst();
        if (head != null) action.accept(head);
        return head != null;
    }

    @Override
    public void forEachRemaining(Consumer<? super TimeSeriesCollection> action) {
        for (;;) {
            update();
            if (pending.isEmpty()) return;

            for (TimeSeriesCollection head = pending.pollFirst();
                 head != null;
                 head = pending.pollFirst()) {
                action.accept(head);
            }
        }
    }

    @Override
    public Spliterator<TimeSeriesCollection> trySplit() {
        if (pending != null) return null; // We're already traversing.

        final Duration delta = new Duration(begin, end).dividedBy(2);
        if (delta.isShorterThan(MIN_INTERVAL)) return null;

        final DateTime forkedBegin = begin;
        begin = begin.plus(delta);
        return new ForwardStream(selector, forkedBegin, begin);
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return SPLITERATOR_CHARACTERISTICS;
    }

    private void update() {
        if (pending == null) pending = new ArrayDeque<>();
        if (!pending.isEmpty()) return;

        while (!begin.isAfter(end)) {
            DateTime searchEnd = begin.plus(MIN_INTERVAL);
            if (searchEnd.isAfter(end)) searchEnd = end;

            selector.execute(begin, searchEnd)
                    .forEach(pending::addLast);
            assert isSorted(pending);
            begin = searchEnd;
            if (!pending.isEmpty()) return;

            final Query betterBeginQuery = new Query(String.format("select * from /.*/ where time > %s and time <= %s order by time asc limit 1", begin.toString(), end.toString()), selector.getDatabase());
            final QueryResult betterBeginQResult = selector.getInfluxDB().query(betterBeginQuery, TimeUnit.MILLISECONDS);
            throwOnResultError(betterBeginQResult);
            begin = extractTimestamps(betterBeginQResult)
                    .min(Comparator.naturalOrder())
                    .orElse(end.plus(Duration.millis(1)));
        }
    }
}
